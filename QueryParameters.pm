package BeaconPlus::QueryParameters;

use Data::Dumper;
use CGI::Simple;
use BeaconPlus::ConfigLoader;

require Exporter;
@ISA    =   qw(Exporter);
@EXPORT =   qw(
  new
  read_param_config
  convert_api_request
  map_scoped_params
  get_variant_params
  norm_variant_params
  check_variant_params
  create_variant_query
  create_subsets_queries
  create_handover_query
);

sub new {

  use File::Basename;

=markdown
The _BeaconPlus_ environment utilizes the _Beacon_ protocol for federated genomic
variant queries, extended by methods discussed in the Beacon API development
and custom extensions which may - or may not - make it into the Beacon
specification but help to increase the usability of the
[Progenetix](http://progenetix.org) resource.

=cut

  my $class     =   shift;

  my $self      =   {
    here_path       =>  File::Basename::dirname( eval { ( caller() )[1] } ),
    query_errors    =>  [],
    parameters      =>  {},
    queries         =>  {},
    cgi             =>  CGI::Simple->new,
  };

  bless $self, $class;

  if ($self->{cgi}->param(debug) > 0) {
    print 'Content-type: text/plain'."\n\n" }

#  $self->read_beacon_specs();
  $self->read_param_config();
  $self->deparse_query_string();
  $self->convert_api_request();
  $self->scope_filters();
  $self->map_scoped_params();
  $self->norm_variant_params();
  $self->check_variant_params();
  $self->create_variant_query();
  $self->create_sample_queries();
  $self->create_subsets_queries();
  $self->create_handover_query();

  return $self;

}

################################################################################

sub read_beacon_specs {

=markdown
### Reading the Beacon Specification

While the specification _in principle_ follows the Beacon specification, and
offers a minimal method to access it, this optioned isn't used in practice due
to the "forward looking" nature of some of the BeaconPlus methods.

=cut

  use YAML::XS qw(LoadFile);

  my $query     =   shift;
  $query->{beacon_spec} =   LoadFile($query->{here_path}.'/specification/beacon.yaml');
  return $query;

}

################################################################################

sub read_param_config {

  use YAML::XS qw(LoadFile);

  my $query     =   shift;
  $query->{config}  =   LoadFile($query->{here_path}.'/config/query_params.yaml');
  return $query;

}

################################################################################

sub deparse_query_string {

=markdown

The query string is deparsed into a hash reference, in the "$query" object,
with the conventions of:

* each parameter is treated as containing a list of values
* values are __split into a list by the comma character__; so an example of
    `key=val1&key=val2,val3`
  would be deparsed to
    `key = [val1, val2, val3]`

The treatment of each attribute as pointing to an array leads to a consistent,
though sometimes awkward, access to the values; even consistently unary values
have to be addressed as (first) array element.

=cut

  my $query     =   shift;

  foreach my $qkey ($query->{cgi}->param()) {

    my @qvalues =   $query->{cgi}->param($qkey);
    if ($qkey =~ /\w/ && grep{ /./ }  @qvalues) {
      foreach my $val (grep{ /./} split(',', join(',', @qvalues))) {
        push(@{ $query->{param}->{$qkey} }, $val);
      }
    }
  }

  return $query;

}

################################################################################

sub convert_api_request {

=pod

=cut

  my $query     =   shift;

  # TODO: in yaml?
  my @request    =    grep{ /\w/ } split('/', $ENV{REQUEST_URI});

  if ($request[0] !~ /^api$/i) { return $query }

  shift @request;  # remove the api part
  foreach (@{$query->{config}->{api_mappings}}) {
    $query->{param}->{ $_->{paramkey} }  =     [ $_->{default} ];
    if ($request[0] =~ /^\?/i)   { last }
    if ($request[0] !~ /\w/i)   {  last }
    $query->{param}->{ $_->{paramkey} }  =  [ shift @request ];

  }

  return $query;

}


################################################################################

sub scope_filters {

  my $query     =   shift;

  foreach my $filterV  (@{ $query->{param}->{filters} }) {
    foreach my $pre  (keys %{ $query->{config}->{filter_prefix_mappings} }) {
      if ($filterV  =~   /^$pre(\:|\-|$)/) {
        if ($query->{config}->{filter_prefix_mappings}->{$pre}->{remove_prefix}) {
          $filterV  =~  s/^$pre(\:|\-)?// }
        push(
          @{ $query->{param}->{ $query->{config}->{filter_prefix_mappings}->{$pre}->{parameter} } },
          $filterV
        );
  }}}

  return $query;

}

################################################################################

sub map_scoped_params {

  my $query     =   shift;

  foreach my $scope (keys %{ $query->{config}->{scopes} }) {
    my $thisP   =   $query->{config}->{scopes}->{$scope}->{parameters};
    foreach my $q_param (grep{ /\w/ } keys %{ $thisP }) {
      my $dbK   =   $thisP->{$q_param}->{dbkey} =~ /\w/ ? $thisP->{$q_param}->{dbkey} : $q_param;
      foreach my $alias ($q_param, $thisP->{$q_param}->{paramkey}, $thisP->{$q_param}->{dbkey}, @{ $thisP->{$q_param}->{alias} }) {
      foreach my $val (@{ $query->{param}->{$alias} }) {
         if ($thisP->{$q_param}->{type} =~/(?:num)|(?:int)|(?:float)/i) {
            $val  =~  tr/[^\d\.\-]//;
             if (grep{ $q_param =~ /$_/ } qw(start end)) { $val =~ s/[^\d]//g }
            $val  *=  1 }
          if ($val =~ /./) {
            if ($val =~ /$thisP->{$q_param}->{pattern}/) {
              if ($thisP->{$q_param}->{type} =~ /array/i) {
                push(@{ $query->{parameters}->{$scope}->{$dbK} }, $val);
              }
              else {
                $query->{parameters}->{$scope}->{$dbK}  =   $val;
  }}}}}}}

  return $query;

}

################################################################################

sub norm_variant_params {

  my $query     =   shift;

  # creating the intervals for range queries, while checking for right order
  # this also fills in min = max if only one parameter has been provided
  # for start or end, respectively
  my @rangeVals =   ();

  foreach my $side (qw(start end)) {
    my $parKeys =   [ grep{ /^$side(?:_m(?:(?:in)|(?:ax)))?$/ } keys %{ $query->{parameters}->{variants} } ];
    my @parVals =   grep{ /^\d+?$/ } @{ $query->{parameters}->{variants} }{ @$parKeys };
    @parVals    =   sort {$a <=> $b} @parVals;
    $query->{parameters}->{variants}->{$side.'_range'}  =  [ $parVals[0], $parVals[-1] ];
    push(@rangeVals, $parVals[0], $parVals[-1]);
  }

  @rangeVals    =  sort {$a <=> $b} grep{  /^\d+?$/ } @rangeVals;
  $query->{parameters}->{variants}->{pos_range} =   [ $rangeVals[0], $rangeVals[-1] ];

  $query->{parameters}->{variants}->{reference_name}    =~  s/chr?o?//i;

  return $query;

}

################################################################################

sub check_variant_params {

  my $query     =   shift;

  # TODO: Use the Beacon specificaion for allowed values

  if ( $query->{parameters}->{variants}->{variant_type} =~ /^D(?:UP)|(?:EL)$/ && ( $query->{parameters}->{variants}->{start_range}->[0] !~ /^\d+?$/ || $query->{parameters}->{variants}->{end_range}->[0] !~ /^\d+?$/ ) ) {
    push(@{ $query->{query_errors} }, 'ERROR: "startMin" (and also startMax) or "endMin" (and also endMax) did not contain a numeric value - both are required for DUP & DEL.') }

  if ( $query->{parameters}->{variants}->{variant_type} =~ /^BND$/ && ( $query->{parameters}->{variants}->{start_range}->[0] !~ /^\d+?$/ && $query->{parameters}->{variants}->{end_range}->[0] !~ /^\d+?$/ ) ) {
    push(@{ $query->{query_errors} }, 'ERROR: Neither "startMin" (and also startMax) or "endMin" (and also endMax) did contain a numeric value - one range is required for BND.') }

  if ($query->{parameters}->{variants}->{reference_name} !~ /^(?:(?:(?:1|2)?\d)|x|y)$/i) {
    push(@{ $query->{query_errors} }, 'ERROR: "referenceName" did not contain a valid value (e.g. "chr17" "8", "X").') }

  if ( $query->{parameters}->{variants}->{variant_type} !~ /^(?:DUP)|(?:DEL)|(?:BND)$/ && $query->{parameters}->{variants}->{alternate_bases} !~ /^[ATGCN]+?$/ ) {
    push(@{ $query->{query_errors} }, 'ERROR: There was no valid value for either "alternateBases or variantType".'); }

  return $query;

}

################################################################################

sub create_variant_query {

  my $query     =   shift;

  if ($query->{parameters}->{variants}->{variant_type} =~ /^D(?:UP)|(?:EL)$/i) {
    $query->create_cnv_query() }
  elsif ($query->{parameters}->{variants}->{variant_type} =~ /^BND$/i) {
    $query->create_bnd_query() }
  elsif ($query->{parameters}->{variants}->{alternate_bases} =~ /^[ATGCN]+?$/) {
    $query->create_precise_query() }

  return $query;

}

################################################################################

sub create_cnv_query {

  my $query     =   shift;

  $query->{queries}->{variants} =   {
    '$and'    => [
      { reference_name      =>  $query->{parameters}->{variants}->{reference_name} },
      { variant_type        =>  $query->{parameters}->{variants}->{variant_type} },
      { start =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
      { start =>  { '$lte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
      { end   =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{end_range}->[0] } },
      { end   =>  { '$lte'  =>  1 * $query->{parameters}->{variants}->{end_range}->[1] } },
    ],
  };

  return $query;

}

################################################################################

sub create_bnd_query {

  my $query     =   shift;

  $query->{queries}->{variants} =   {
    '$and'      => [
      { reference_name  =>  $query->{parameters}->{variants}->{reference_name} },
      { '$or' =>  [
        { variant_type  =>  'DUP' },
        { variant_type  =>  'DEL' },
        { variant_type  =>  'BND' },
      ] },
      { '$or'   =>  [
        { '$and'=> [
            { start =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
            { start =>  { '$lte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
          ]
        },
        { '$and'=> [
            { end   =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
            { end   =>  { '$lte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
          ]
        },
      ] },
    ],
  };

  return $query;

}

################################################################################

sub create_precise_query {

  my $query     =   shift;

  if ($query->{parameters}->{variants}->{alternate_bases} =~ /N/) {
    $query->{parameters}->{variants}->{alternate_bases} =~  s/N/./g;
    $query->{parameters}->{variants}->{alternate_bases} =   qr/^$query->{parameters}->{variants}->{alternate_bases}$/;
  }

  my @qList     =   (
    { reference_name  =>  $query->{parameters}->{variants}->{reference_name} },
    { alternate_bases =>  $query->{parameters}->{variants}->{alternate_bases} },
    { start =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{pos_range}->[0] } },
    { start =>  { '$lte'  =>  1 * $query->{parameters}->{variants}->{pos_range}->[-1] } },
  );

  if ($query->{parameters}->{variants}->{reference_bases} =~ /^[ATCG]+?$/) {
    push(
      @qList,
      { reference_bases =>  $query->{parameters}->{variants}->{reference_bases} },
    );
  }

  $query->{queries}->{variants} =   { '$and' => \@qList };
  return $query;

}

################################################################################

sub create_sample_queries {

=markdown

#### Sample (_biosamples_ and _callsets_) Queries

Queries with multiple options for the same attribute are treated as logical "OR".
=cut

  my $query     =   shift;

  foreach my $scope (qw(biosamples callsets)) {

    my @qList;

    foreach my $qKey (keys %{ $query->{parameters}->{$scope} }) {
      my %thisQlist;
      if (ref $query->{parameters}->{$scope}->{$qKey} eq 'ARRAY') {
        foreach (@{ $query->{parameters}->{$scope}->{$qKey} }) {
          $thisQlist{ $qKey.'::'.$_ }  =    { $qKey => qr/^$_/i };
        }
      }
      else {
        $thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => qr/^$query->{parameters}->{$scope}->{$qKey}/i } }

      if (scalar keys %thisQlist == 1)    { push(@qList, (values %thisQlist)[0]) }
      elsif (scalar keys %thisQlist > 1)  { push(@qList, {'$or' => [ values %thisQlist ] } ) }
    }

=markdown

The construction of the query object depends on the detected parameters:

* if empty list => no change, empty object
* if 1 parameter => direct use
* if several parameters are queried => connection through the MongoDB  "$and" constructor

=cut

    if (@qList == 1)    { $query->{queries}->{$scope} =   $qList[0] }
    elsif (@qList > 1)  { $query->{queries}->{$scope} =   { '$and' => \@qList } }

  }

  return $query;

}

################################################################################

sub create_subsets_queries {

  my $query     =   shift;

  foreach my $scope (qw(biosubsets datacollections publications)) {
    my @qList;

    foreach my $qKey (keys %{ $query->{parameters}->{$scope} }) {
      my @thisQlist;

      if (ref $query->{parameters}->{$scope}->{$qKey} eq 'ARRAY') {
        foreach (@{ $query->{parameters}->{$scope}->{$qKey} }) { push(@thisQlist, { $qKey => qr/^$_/i }) } }
      else {

        my $val  =  $query->{parameters}->{$scope}->{$qKey};

        if ($val =~ /^(<|>\=?)(\d+?(\.\d+?)?)$/) {
          my ($rel, $num)  =    ($1, 1 * $2);
          if ($rel eq '>') {
            push(@thisQlist, { $qKey => { '$gt' => $num } } ) }
          if ($rel eq '<') {
            push(@thisQlist, { $qKey => { '$lt' => $num } } ) }
          if ($rel eq '>=') {
            push(@thisQlist, { $qKey => { '$gte' => $num } } ) }
          if ($rel eq '<=') {
            push(@thisQlist, { $qKey => { '$lte' => $num } } ) }
        } else {
          push(@thisQlist, { $qKey => qr/^$val/i } ) }
      }

      if (@thisQlist == 1)    { push(@qList, $thisQlist[0]) }
      elsif (@thisQlist > 1)  { push(@qList, {'$or' => [ @thisQlist ] } ) }
    }

    if (@qList == 1)    { $query->{queries}->{$scope} =   $qList[0] }
    elsif (@qList > 1)  { $query->{queries}->{$scope} =   { '$and' => \@qList } }

  }

  return $query;

}

################################################################################

sub create_handover_query {

  my $query     =   shift;

  if (! $query->{parameters}->{handover}->{_id}) { return $query }
  if ($query->{parameters}->{handover}->{_id} !~ /$query->{config}->{scopes}->{handover}->{accessid}->{pattern}/) { return $query }

  $query->{queries}->{handover} =   { _id  =>  $query->{parameters}->{handover}->{_id} };

  return $query;

}

################################################################################

1;
