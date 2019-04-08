package BeaconPlus::QueryExecution;

use Data::Dumper;
use UUID::Tiny;
use PGX::Helpers::UtilityLibs;
use BeaconPlus::ConfigLoader;

require Exporter;
@ISA    =   qw(Exporter);
@EXPORT =   qw(
  new
  prefetch_data
  create_handover_object
  execute_aggregate_query
);

sub new {

  use File::Basename;
  $MongoDB::Cursor::timeout = 120000;

=pod

=cut

  my $class     =   shift;
  my $config    =   shift;

  my $self      =   {
    config      =>  $config,
    handover_coll   =>   MongoDB::MongoClient->new()->get_database( $config->{handover_db} )->get_collection( $config->{handover_coll} ),
  };

  bless $self, $class;
  return $self;

}

################################################################################

sub prefetch_data {

  my $prefetch  =   shift;
  my $method    =   shift;
  my $query     =   shift;
  my $filters   =   shift;
  
  $prefetch->{dataset}  ||= 'arraymap';
  $prefetch->{filters}  ||= {};
  my (
    $source_c,
    $source_k,
    $target_c,
    $target_k
  )             =   split('::', $method);

  # two components are interpolated to same output
  if ($target_c !~  /\w/) { $target_c = $source_c }
  if ($target_k !~  /\w/) { $target_k = $source_k }

  my $distincts =   $prefetch->{db_conn}->run_command([
                      "distinct"=>  $source_c,
                      "key"     =>  $source_k,
                      "query"   =>  $query,
                    ]);

  my $distVals  =   $distincts->{values};
  if ($prefetch->{filters}->{randno} =~ /^\d+?$/) {
    $distVals   =   RandArr($distVals, $prefetch->{filters}->{randno}) }
  my $distCount =   scalar @{ $distVals };

  $prefetch->{handover}->{$method}  =   {
    source_db         =>  $prefetch->{dataset},
    source_collection =>  $source_c,
    source_key        =>  $source_k,
    target_collection =>  $target_c,
    target_key        =>  $target_k,
    target_values     =>  $distVals,
    target_count      =>  $distCount,
  };

  return $prefetch;

}

################################################################################

sub get_base_counts {

  my $prefetch  =   shift;
  
  $prefetch->{dataset}  ||= 'arraymap';
  $prefetch->{filters}  ||= {};

  $prefetch->{counts}   =   {};
  foreach (qw(callsets biosamples variants)) {
    $prefetch->{counts}->{$_.'_base_count'}   =   $prefetch->{db_conn}->get_collection($_)->count();
  }
  return $prefetch;

}

################################################################################

sub create_handover_object {

  my $prefetch  =   shift;
  my $method    =   shift;
  my $query     =   shift;

  $prefetch->prefetch_data($method, $query);
  $prefetch->{handover}->{$method}->{_id} =   create_UUID_as_string();

  $prefetch->{handover_coll}->insert( $prefetch->{handover}->{$method} );

  return $prefetch;

}

################################################################################

sub execute_aggregate_query {

  my $prefetch  =   shift;
  my $query     =   shift;
  
  $prefetch->get_base_counts();

  # the main prefetch method here is the retrieval of the 
  # "callsets" collection's "id" values
  my $method    =   'callsets::id';
  
  if (! grep{ /.../ }
    keys %{ $query->{callset_query} },
    keys %{ $query->{queries}->{biosamples} },
    keys %{ $query->{queries}->{variants} }
  ) { return }

# 1. Checking for a callsets query & return iq query but no matches

  if (grep{ /.../ } keys %{ $query->{callset_query} } ) {
    $prefetch->prefetch_data( $method, $query->{callset_query} );
    if ($prefetch->{handover}->{$method}->{target_count} < 1) { 
      return $prefetch } 
  }
  
# 2. Checking for a biosamples query & return if query but no matches

  if (grep{ /.../ } keys %{ $query->{queries}->{biosamples} } ) {
    my $thisM   =   'biosamples::id';
    $prefetch->prefetch_data( $thisM, $query->{queries}->{biosamples} );
    $prefetch->{counts}->{biosamples_query_match_count}  =   $prefetch->{handover}->{$thisM}->{target_count};
    if ($prefetch->{handover}->{$thisM}->{target_count} < 1) { 
      return $prefetch }
# 3. If biosamples matches, retrieve the callset_id values; if there had been 
#    matches before => intersect through added "$in" query

    my $thisQ   =   { 'biosample_id' => { '$in' =>  $prefetch->{handover}->{'biosamples::id'}->{target_values} } };
    if ($prefetch->{handover}->{$method}->{target_count} > 0) {
      $thisQ      =   { '$and' =>
        [
          $thisQ,
          { $prefetch->{handover}->{$method}->{target_key} => { '$in' =>  $prefetch->{handover}->{$method}->{target_values} } },
        ],
      };
    }
    $prefetch->prefetch_data( $method, $thisQ );
    if ($prefetch->{handover}->{$method}->{target_count} < 1) { 
      return $prefetch } 
  }

# 4. Checking for a variants query; if there had been callset matches before
#    => intersect through added "$in" query; return iq query but no matches

  if (grep{ /.../ } keys %{ $query->{queries}->{variants} } ) {
    my $thisQ   =   $query->{queries}->{variants};
    if ($prefetch->{handover}->{$method}->{target_count} > 0) {

      $thisQ    =   { '$and' =>
        [
          $thisQ,
           { 'callset_id' => { '$in' =>  $prefetch->{handover}->{$method}->{target_values} } },
        ],
      };
    }
    $prefetch->create_handover_object( 'variants::_id', $thisQ );

    if ($prefetch->{handover}->{'variants::_id'}->{target_count} < 1) { 
      return $prefetch }

    $thisQ      =    { '_id' => { '$in' => $prefetch->{handover}->{'variants::_id'}->{target_values} } };
    $thisM      =   'variants::callset_id';
    $prefetch->prefetch_data( $thisM, $thisQ );
    
    # just to overwrite the key with the standard $method
    $prefetch->{handover}->{$method}->{target_values}  =   $prefetch->{handover}->{$thisM}->{target_values};
    $prefetch->{handover}->{$method}->{target_count}  =   $prefetch->{handover}->{$thisM}->{target_count};

    # getting the distinct variants
    $thisQ      =    { '_id' => { '$in' => $prefetch->{handover}->{'variants::_id'}->{target_values} } };
    $thisM      =   'variants::digest';
    $prefetch->prefetch_data( $thisM, $thisQ );

  }

# Up to here, queries against callsets, biosamples and variants have all been reduced to the biosamples::id values.

# 6. Reset the biosample handover object & store it, from the current callsets

  $prefetch->prefetch_data(
    'callsets::biosample_id',
    { 'id' => { '$in' => $prefetch->{handover}->{$method}->{target_values} } },
  );
#print '<hr/>callsets::biosample_id: '.$prefetch->{handover}->{'callsets::biosample_id'}->{target_count};
  
  $prefetch->create_handover_object(
    'biosamples::_id',
    { 'id' => { '$in' => $prefetch->{handover}->{'callsets::biosample_id'}->{target_values} } },
  );
  $prefetch->create_handover_object(
    'callsets::_id',
    { 'id' => { '$in' => $prefetch->{handover}->{$method}->{target_values} } },
  );
  
  foreach (qw(callsets biosamples variants)) {

    if ($prefetch->{handover}->{$_.'::_id'}->{target_count}) {
      $prefetch->{counts}->{$_.'_match_count'} =   $prefetch->{handover}->{$_.'::_id'}->{target_count}; 
    }
  }
 
  return $prefetch;

}

1;
