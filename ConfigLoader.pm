package BeaconPlus::ConfigLoader;

use File::Basename;
use YAML::XS qw(LoadFile);

require Exporter;
@ISA    =   qw(Exporter);
@EXPORT =   qw(
  new
  _dw
);

sub new {

=pod

=cut

  my $class     =   shift;  
  my $self      =   LoadFile(File::Basename::dirname( eval { ( caller() )[1] } ).'/config/config.yaml') or die print 'Content-type: text'."\n\n¡No config.yaml file in this path!";
  bless $self, $class;
  if ($ENV{SERVER_NAME} =~ /\.test$|\//) { $self->{url_base} =~  s/\.org/.test/ }

  return $self;

}

################################################################################

sub _dw {
  use Data::Dumper;
	print	'Content-type: text/html'."\n\n";
  print Dumper(@_);
}

################################################################################

1;
