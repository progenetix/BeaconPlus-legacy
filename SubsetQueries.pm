package BeaconPlus::SubsetQueries;

use Data::Dumper;
use UUID::Tiny;
use PGX::Helpers::UtilityLibs;
use BeaconPlus::ConfigLoader;

require Exporter;
@ISA            =   qw(Exporter);
@EXPORT         =   qw(
  new
  get_matching_subsets
);

sub new {

  use MongoDB;
  use MongoDB::MongoClient;
  $MongoDB::Cursor::timeout = 120000;

=podmd
This library provides non-Beacon standard  _convenience_ functionality for
the Progenetix Beacon environment to access data collections

* `biosubsets`
* `datacollections`

... which are used to provide pre-computed, aggregate from e.g. all samples
belonging to a single publication or representing a specific diagnostic code.

=cut

  my $class     =   shift;
  my $config    =   shift;
  my $dataset   =   $config->{dataset_names}->[0];

  my $self      =   {
    config      =>  $config,
    dataset			=>	$dataset,
    queries			=>	$config->{queries},
    db_conn			=>	MongoDB::MongoClient->new()->get_database( $dataset ),
    handover_coll   =>   MongoDB::MongoClient->new()->get_database( $config->{handover_db} )->get_collection( $config->{handover_coll} ),
  };

  bless $self, $class;
  return $self;

}

################################################################################

sub get_matching_subsets {

  my $subsets   =   shift;

  foreach my $scope (qw(biosubsets datacollections)) {

    if (! grep{ /../ } keys %{ $subsets->{queries}->{ $scope } }) { next }

    my $query		=		$subsets->{queries}->{ $scope };

    # first request is done without frequencymaps return, for listing
    my $subsetFields  =   { _id => 0 };
    foreach (qw(id label count child_terms frequencymaps)) {
      $subsetFields->{$_} = 1 }

    $subsets->{all} =   {};
		my $cursor   =   $subsets->{db_conn}->get_collection( $scope )->find( $query )->fields($subsetFields);
		foreach my $subset ($cursor->all) {
			if (! grep{/.../} @{$subset->{child_terms}}) { $subset->{child_terms} =   [ $subset->{id} ] }
			push(
				@{ $subsets->{ $subsets->{dataset} }->{subsets} },
				$subset
			);
			$subsets->{all}->{ $subset->{id} }  =   {
				id      =>  $subset->{id},
				label   =>  $subset->{label},
			};
		}
  }

  return $subsets;

}

1;
