#!/usr/bin/perl

# Beacon+ support scripts
# © 2017-2019 Michael Baudis: m@baud.is

use strict;
use CGI::Carp qw(fatalsToBrowser);
use CGI qw(param);

use JSON;
use MongoDB;
use MongoDB::MongoClient;
$MongoDB::Cursor::timeout = 120000;

use Data::Dumper;

# local packages
use BeaconPlus::ConfigLoader;
use BeaconPlus::QueryParameters;
use BeaconPlus::QueryExecution;

use lib './PGX';
use PGX;

=podmd
The __BeaconHandover__ is a utility server side application to provide 
__h-&gt;o__ functionality to the _BeaconPlus_ environment.

The script accepts following parameters:

* `accessid`
    - the `_id` of an entry in the `___handover_db___.___handover_coll___` 
    database, which in the standard implementation stores pointers to the 
    results from a _BeaconPlus_ query
* `do`
    - an action from `handover_types`
* `jsonpretty`
    - formatting instruction for JSON style
    - defaults to '0', i.e. no line breaks etc.
    - values of 1 or y or pretty will provide an formatted return

=cut

my $config      =   BeaconPlus::ConfigLoader->new();
my $error				=		q{};
our $handover		=		{};
my $pretty     	=   $config->{param}->{jsonpretty}->[0];
if ($pretty !~ /^1|y|pretty/) {
  $pretty       =   0 }
else {
  $pretty       =   1 }


if (! grep{ /../ } keys %{ $config->{queries}->{handover} }) { 
  $error				= '¡ Wrong or missing access_id parameter !' }

if (! grep{ $config->{param}->{do}->[0] eq $_ } keys %{ $config->{handover_types} }) { 
  $error				= '¡ Wrong or missing "do" parameter !' }

if ($error  !~  /\w/) {
	$handover 		=    MongoDB::MongoClient->new()->get_database( $config->{handover_db} )->get_collection( $config->{handover_coll} )->find_one( $config->{queries}->{handover} ) }

if (! $handover->{_id} ) { 
  $error				= '¡ No handover object found !' }


if ($error  =~  /\w/) {
  print 'Content-type: text'."\n\n".$error;
  exit;
}

_print_histogram();
_export_callsets();
_export_biosamples_individuals();
_export_variants();
_display_variants_in_UCSC();

exit;

################################################################################
# subs #########################################################################
################################################################################

sub _print_histogram {

  if ($config->{param}->{do}->[0] !~ /histo/i) { return }

  $config->{param}->{-plotid}		=		'histoplot';
  $config->{param}->{-text_bottom_left} 	=  $handover->{source_db}.': '.$handover->{target_count}.' samples';
  
  my $pgx       =   new PGX($config->{param});
  $pgx->pgx_open_handover($config, $config->{param}->{accessid}->[0]);
	$pgx->pgx_samples_from_handover();
  $pgx->pgx_add_frequencymaps( [ { statusmapsets =>  $pgx->{samples} } ] );
  $pgx->return_histoplot_svg();

  print 'Content-type: image/svg+xml'."\n\n";
  print $pgx->{svg};
  exit;

}

################################################################################

sub _print_array {

# TODO ...

  if ($config->{param}->{do}->[0] !~ /array/i) { return }

	my $plotargs    =   { map{ $_ => join(',', @{ $config->{param}->{$_} }) } (grep{ /^\-\w+?$/ } keys %{ $config->{param} }) };
	$config->{param}->{-plotid}			=	 'arrayplot';
	$config->{param}->{-plottype}		=	 'array';
  
  my $pgx       =   new PGX($config->{param});
  $pgx->pgx_open_handover($config, $config->{param}->{accessid}->[0]);
	$pgx->pgx_samples_from_handover();
  $pgx->{parameters}->{text_bottom_left} 	=  $pgx->{samples}->[0]->{id};
  $pgx->pgx_add_frequencymaps( [ { statusmapsets =>  $pgx->{samples} } ] );
  $pgx->return_histoplot_svg();

  print 'Content-type: image/svg+xml'."\n\n";
  print $pgx->{svg};
  exit;

}

################################################################################

sub _export_callsets {

  if ($config->{param}->{do}->[0] !~ /callsetsdata/i) { return }

  print 'Content-type: application/json'."\n\n";

  my $datacoll  =   MongoDB::MongoClient->new()->get_database( $handover->{source_db} )->get_collection( $handover->{target_collection} );
  my $dataQuery =   { $handover->{target_key} => { '$in' => $handover->{target_values} } };
  my $cursor	  =		$datacoll->find( $dataQuery )->fields( { _id => 0, updated => 0, created => 0 } );

  print	JSON::XS->new->pretty( $pretty )->allow_blessed->convert_blessed->encode([$cursor->all]);
  exit;

}

################################################################################

sub _export_biosamples_individuals {


  if (! grep{ /^$config->{param}->{do}->[0]$/ } qw(biosamplesdata individualsdata)) { return }

  print 'Content-type: application/json'."\n\n";

  my $dataconn  =   MongoDB::MongoClient->new()->get_database( $handover->{source_db} );

  my $datacoll  =   $dataconn->get_collection($handover->{target_collection});
  my $cursor	  =		$datacoll->find( { $handover->{target_key} => { '$in' => $handover->{target_values} } } )->fields( { attributes => 0, _id => 0, updated => 0, created => 0 } );

  print	JSON::XS->new->pretty( $pretty )->allow_blessed->convert_blessed->encode([$cursor->all]);
  exit;

}

################################################################################

sub _export_variants {

  if ($config->{param}->{do}->[0] !~ /variants/) { return }

  print 'Content-type: application/json'."\n\n";

  my $dataconn  =   MongoDB::MongoClient->new()->get_database( $handover->{source_db} );
  my $datacoll  =   $dataconn->get_collection( 'variants' );
  my $cursor    =   {};

  my $key       =   $handover->{target_key};
  my $values    =   $handover->{target_values};

  if ($config->{param}->{do}->[0] =~ /callset/i) { 
    $key        =   'callset_id';
    my $distincts =   $dataconn->run_command([
                      "distinct"=>  'callsets',
                      "key"     =>  'id',
                      "query"   =>  { _id => { '$in' => $handover->{target_values} } },
                    ]);

    $values     =   $distincts->{values};

  }
  $cursor	      =		$datacoll->find( { $key => { '$in' => $values } } )->fields( { _id => 0, updated => 0, created => 0 } );

  print	JSON::XS->new->pretty( $pretty )->allow_blessed->convert_blessed->encode([$cursor->all]);
  exit;

}

################################################################################

sub _display_variants_in_UCSC {

  print 'Content-type: text/html'."\n\n";

  use BeaconPlus::DataExporter;

  if ($config->{param}->{do}->[0] !~ /ucsc/i) { return }

  write_variants_bedfile($config, $handover);
  
  my $ucscPos =   'chr'.$config->{param}->{ucscChro}->[0].':'.$config->{param}->{ucscStart}->[0];
  if ($config->{param}->{ucscEnd}->[0] >= $config->{param}->{ucscStart}->[0]) {
    $ucscPos  .=  '-'. $config->{param}->{ucscEnd}->[0] }

  my $UCSCurl =   'http://genome.ucsc.edu/cgi-bin/hgTracks?ignoreCookie=1&org=human&db=hg38&position='.$ucscPos.'&hgt.customText='.$config->{url_base}.'/tmp/'.$handover->{_id}.'.bed';

  print '<html>
<meta http-equiv="refresh" content="0; url='.$UCSCurl.'" />
<a href="'.$UCSCurl.'">'.$UCSCurl.'</a></html>'."\n";
  
  exit;

}

################################################################################

1;
