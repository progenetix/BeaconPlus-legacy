package BeaconPlus::DataExporter;

use Data::Dumper;
use PGX::Helpers::UtilityLibs;

require Exporter;
@ISA    =   qw(Exporter);
@EXPORT =   qw(
  new
  create_handover_exporter
);

sub new {

=pod

=cut

  my $class     =   shift;
  my $prefetch  =   shift;

  my $self      =   {
    config      =>  $prefetch->{config},
    handover_pre    =>  $prefetch->{handover},
    handover    =>  [],
  };

  bless $self, $class;
  return $self;

}

################################################################################

sub create_handover_exporter {

  my $exporter  =   shift;
    
  foreach my $h_o (sort keys %{ $exporter->{config}->{handover_types} }) {
  
    my $handoverType  =   $exporter->{config}->{handover_types}->{$h_o};
    my $handoverMeth  =   $handoverType->{handover_method};
    my $handoverPre   =   $exporter->{handover_pre}->{ $handoverMeth };

    if ($handoverPre->{target_count} < 1) { next }

    my $urlBase =   $exporter->{config}->{url_base};
    if ($handoverType->{script_path_web} !~ /cgi/) {
      $handoverType->{script_path_web}  =   '/beaconplus-server/beacondeliver.cgi' }    
      
    if ($handoverType->{script_path_web} !~ /beacon/) {
      $urlBase  =~   s/\/\/beacon\./\/\// }    
      
    push(
      @{ $exporter->{handover} },
      {
        handoverType  =>  {
          id    =>  $handoverType->{id},
          label =>  $handoverType->{label}      
        },
        description =>  $handoverType->{description},
        url     =>  $urlBase.$handoverType->{script_path_web}.'?do='.$h_o.'&accessid='.$handoverPre->{_id},
      }
    );
  
  }

  return $exporter;

}

1;
