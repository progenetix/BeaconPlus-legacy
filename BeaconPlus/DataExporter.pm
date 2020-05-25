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

    if ($handoverType->{script_path_web} !~ /cgi/) {
      $handoverType->{script_path_web}  =   '/cgi-bin/beacondeliver.cgi' }

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
        url     =>  $exporter->{handover_url_base}.$handoverType->{script_path_web}.'?do='.$h_o.'&accessid='.$handoverPre->{_id},
      }
    );

  }

  return $exporter;

}

1;
