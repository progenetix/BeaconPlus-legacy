package BeaconPlus::DataExporter;

use Data::Dumper;
use PGX::Helpers::UtilityLibs;

require Exporter;
@ISA            =   qw(Exporter);
@EXPORT         =   qw(
  new
  create_handover_exporter
  write_variants_bedfile
);

sub new {

=pod

=cut

  my $class     =   shift;
  my $config    =   shift;
  my $prefetch  =   shift;

  my $self      =   {
    handover_url_base  =>  $config->{url_base},
    handover_types  =>  $config->{handover_types},
    handover_pre    =>  $prefetch->{handover},
    handover    =>  [],
  };

  bless $self, $class;
  return $self;

}

################################################################################

sub create_handover_exporter {

  my $exporter  =   shift;

  foreach my $h_o (sort keys %{ $exporter->{handover_types} }) {

    my $handoverType  =   $exporter->{handover_types}->{$h_o};
    my $handoverMeth  =   $handoverType->{handover_method};
    my $handoverPre   =   $exporter->{handover_pre}->{ $handoverMeth };

    if ($handoverPre->{target_count} < 1) { next }

    my $h_o_url;
    
    if ($handoverType->{script_path_web} !~ /.../) {
      $handoverType->{script_path_web}  =   '/cgi-bin/beacondeliver.cgi' }

    if ($handoverType->{script_path_web} !~ /beacon/) {
      $urlBase  =~   s/\/\/beacon\./\/\// }

    if ($ENV{SERVER_NAME} =~ /\.test$|\// && $handoverType->{script_path_web} =~ /http/) {
       $handoverType->{script_path_web} =~  s/\.org/.test/ }

=podmd
If a custom URL is provided (e.g. here by previously overriding the 
`$exporter->{handover_types}->{UCSClink}->{url}` value), this value will replace 
the handover `url` value. Alternatively, a URL provided as `script_path_web` 
parameter will be prefixed to the query. If it contains a full address (i.e. 
"http") it will replace the canonical script root (e.g. here used for sending an 
action to the UI instead of the cgi entry point).

=cut

    if ($handoverType->{url} =~ /https?\:\/\/\w/i) {
      $h_o_url  =  $handoverType->{url} }
    else {
      if ($handoverType->{script_path_web} !~ /http/) {
        $handoverType->{script_path_web}  =  $exporter->{handover_url_base}.$handoverType->{script_path_web} }
      $h_o_url  .=  $handoverType->{script_path_web}.'?do='.$h_o.'&accessid='.$handoverPre->{id}.$handoverType->{link_post} }
       
    push(
      @{ $exporter->{handover} },
      {
        handoverType  =>  {
          id    =>  $handoverType->{id},
          label =>  $handoverType->{label}
        },
        description =>  $handoverType->{description},
        url     =>  $h_o_url,
      }
    );

  }

  return $exporter;

}

################################################################################

sub write_variants_bedfile {

=podmd
#### `BeaconPlus::DataExporter::write_variants_bedfile`

##### Accepts

* a BeaconPlus _config_ object
* a BeaconPlus _handover_ object with its `target_values` representing `id` 
objects of a `variants` collection
* the path to an output file
    - optional, otherwise crated in the "tmp" directory of the document root
    
The function creates a basic BED file and returns its local path. A standard 
use would be to create a link to this file and submit it as `hgt.customText` 
parameter to the UCSC browser.

=cut

  my $config    =   shift;
  my $handover  =   shift;
  my $bedFile   =   shift;

  if ($bedFile !~ /\.bed/i) {
    $bedFile    =  $config->{document_base}.'/tmp/'.$handover->{id}.'.bed' }

  if ($handover->{target_count} < 1) { return }

  my $datacoll  =   MongoDB::MongoClient->new()->get_database( $handover->{source_db} )->get_collection( 'variants' );
  my $cursor	  =		$datacoll->find( { _id => { '$in' => $handover->{target_values} } } );
  my @varsAll   =   $cursor->all;

  @varsAll      =   sort{ $b->{info}->{cnv_length} <=> $a->{info}->{cnv_length} } @varsAll;
=podmd
##### TODO

* The creation of the different variant types is still rudimentary and has to be 
expanded in lockstep with improving Beacon documentation and examples. The 
definition of the types and their match patterns should also be moved to a 
+separate configuration entry and subroutine.
* evaluate to use "bedDetails" format

=cut

  my $vars      =   {
    DUP         =>  [ grep{ $_->{variant_type} eq 'DUP' } @varsAll ],
    DEL         =>  [ grep{ $_->{variant_type} eq 'DEL' } @varsAll ],
    SNV         =>  [ grep{ $_->{reference_bases} =~ /^[ACGTN]+$/ } @varsAll ],
  };

  open  FILE, '>'."$bedFile" or die "$bedFile could not be created";
  foreach my $type (keys %$vars) {
    if (@{ $vars->{$type}} > 0) {
      my $col   =   $config->{output_params}->{'color_var_'.lc($type).'_rgb'};
      my $name  =   $type.'variants';
      print FILE <<END;
track name=$name visibility=squish description="$type variants matching the query" color=$col
#chrom  chromStart  chromEnd  biosampleId
END

      foreach my $var (@{ $vars->{$type}}) {
        my ($start, $end) =   (($var->{start_min}), $var->{end_max});
        # shift for UCSC display - 1-based representation
        if ($end <= $start) {
          $end  =   $start + 1 }
        print FILE join("\t", 'chr'.$var->{reference_name},  $start, $end, $var->{biosample_id}.'___'.$var->{digest})."\n"; 
      }  
    }   
  }

  close FILE;

  return $bedFile;

}

1;
