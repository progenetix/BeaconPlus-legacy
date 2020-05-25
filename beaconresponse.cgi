#!/usr/bin/perl

# Progenetix & arrayMap site scripts
# Beacon+ server implementation based on arraymap.org | progenetix.org data
# © 2000-2019 Michael Baudis: m@baud.is

use strict;
use CGI::Carp qw(fatalsToBrowser);
use CGI qw(param);
use Data::Dumper;
use JSON;
use MongoDB;
use MongoDB::MongoClient;
$MongoDB::Cursor::timeout = 36000;

use BeaconPlus::ConfigLoader;
use BeaconPlus::QueryParameters;
use BeaconPlus::QueryExecution;
use BeaconPlus::DataExporter;

=podmd
The "beaconresponse.cgi" script is a server backend for the Beacon protocol,
specifically adapted to connect to _MongoDB_ database backends that adhere to
the core GA4GH model.

#### Example Queries

* <https://beacon.progenetix.org/cgi-bin/beaconresponse.cgi?datasetIds=arraymap&datasetIds=progenetix&referenceName=9&assemblyId=GRCh38&variantType=DEL&startMin=19500000&startMax=21975098&endMin=21967753&endMax=24500000&referenceBases=N&filters=NCIT:C3224>
    - focal deletions in the CDKN2A locus, filtered for "NCIT:C3224" (Melanoma)
    - this query is against 2 datasets (progenetix, arraymap)
    - *this query puts some serious strain on the server!*
* <https://beacon.progenetix.org/cgi-bin/beaconresponse.cgi/?datasetIds=arraymap&referenceName=9&assemblyId=GRCh38&variantType=DEL&startMin=19500000&startMax=21975098&endMin=21967753&endMax=24500000&referenceBases=N&filters=icdom-94003>
    - as above, but using "icdom-94003", the internal code for ICD-O 3 "9400/3" (Glioblastoma, NOS)
* <https://beacon.progenetix.org/cgi-bin/beaconresponse.cgi?datasetIds=dipg&referenceName=17&assemblyId=GRCh38&startMin=7572826&endMax=7579005&referenceBases=*&alternateBases=N&filters=icdot-C71.7&>
    - wildcard range query for allelic variants on chromosome 17, between bases 7572826 and 7579005
    - dataset "dipg"
    - Since the current specification requires a `alternateBases` parameter, and doesn't include wildcards *but* allows "N", the query will __only__ return variants where the replacement has a length of 1. "NN" leads to 2 etc. This may be changed in future versions of the protocol.
* <https://beacon.progenetix.org/cgi-bin/beaconresponse.cgi?datasetIds=dipg&referenceName=17&assemblyId=GRCh38&start=7577121&referenceBases=G&alternateBases=A&filters=icdot-C71.7&>
		- the "traditional" precise BeaconAlleleRequest, as supported by the first Beacon version
    - dataset "dipg"  

=cut

my $config      =   BeaconPlus::ConfigLoader->new();

################################################################################

my $datasetR    =   [];
if (
	(! grep{ /ERROR/i } @{ $config->{query_errors}->{variants} })
	||
	$config->{param}->{overrideErrors}->[0] > 0
) {
  foreach (@{ $config->{ dataset_names } }) {
    push(@$datasetR, _getDataset($config, $_) );
  }
} else {
  foreach (@{ $config->{ dataset_names } }) {
    push(@$datasetR, { error => join("\n", @{ $config->{query_errors}->{variants} }) } );
  }
  print "Status: 400 Bad request\n".'Content-type: application/json'."\n\n".JSON::XS->new->encode($datasetR)."\n";

  exit;
}

if (! -t STDIN) { print 'Status: 200'."\n".'Content-type: application/json'."\n\n" }
my $bcExists    =   \0;

foreach (@$datasetR) {
  if ($_->{variantCount} > 0) {
    $bcExists = \1 }
}

my $response    =   {
  beaconId      =>  $config->{id},
#  changeDate    =>  $config->{changeDate},
  exists        =>  $bcExists,
  apiVersion    =>  $config->{apiVersion},
  url           =>  $config->{welcomeUrl},
  info          =>  {
    queryString =>  $ENV{QUERY_STRING},
    version     =>  'Beacon+ implementation based on the development branch of the ELIXIR Beacon project with custom extensions',
  },
};

=podmd
The parameters of the allele request are verified & added to the response.

=cut

my $v_pars_conf =   $config->{q_conf}->{scopes}->{variants}->{parameters};
foreach (keys %{ $v_pars_conf }) {
  if ($config->{param}->{$_}->[0] =~ /$v_pars_conf->{$_}->{pattern}/) {
    $response->{alleleRequest}->{$_}  =   $config->{param}->{$_}->[0];
    if ($v_pars_conf->{$_}->{type} eq 'integer') {
       $response->{alleleRequest}->{$_} *= 1 }
  }
}


if (! $config->{param}->{includeDatasetResponses}->[0]) {
  $datasetR     =   [] }
elsif ($config->{param}->{includeDatasetResponses}->[0] eq 'HIT') {
  $datasetR     =   [ grep{ $_->{variantCount} > 0 } @$datasetR ] }
elsif ($config->{param}->{includeDatasetResponses}->[0] eq 'MISS') {
  $datasetR     =   [ grep{ $_->{variantCount} < 1 } @$datasetR ] }

if (@$datasetR > 0) {
  $response->{datasetAlleleResponses} =   $datasetR }

print JSON::XS->new->pretty( 0 )->encode($response)."\n";

exit;

################################################################################
################################################################################
# SUBs #########################################################################
################################################################################
################################################################################

sub _getDataset {

=pod

=cut

  my $config    =   shift;
  my $dataset   =   shift;

  # Handover:
  my $handover  =   [];
  
  # custom handling of distinct variants
  my $varResp  	=   [];
  my $varDistNo =   0;

=podmd
#### Beacon Query Execution

Different types of query are run, if parameters for them exist.

The current concept is:

- run queries on the different primary object types (individual, biosample, callset, variant)
- provide the object-level counts as output (and store the ids for handover procedures)
- aggregate on biosamples

* Variant query
* Callset query
* Biosample query
* Individual query

The details of the query execution can be found in the documentation for the
[__BeaconPlus::QueryExecution.pm__](/doc/+generated-doc-BeaconPlus-QueryExecution/) module.

=cut

  my $counts    =   {
    frequency   => 	0,
    exists      =>  \0,
  };
  
  my $prefetch  =   BeaconPlus::QueryExecution->new($config, $dataset);
  $prefetch->execute_aggregate_query();
  
  ##############################################################################

  my $exporter  =   BeaconPlus::DataExporter->new($config, $prefetch);

  my $ucscStart =   $config->{param}->{start}->[0] =~ /^\d+?/ ? $config->{param}->{start}->[0] : $config->{param}->{startMin}->[0];
  $ucscStart    +=  1;

  my $ucscEnd;
  if ($config->{param}->{end}->[0] =~ /^\d+?/) {
    $ucscEnd    =   $config->{param}->{end}->[0] }
  elsif ($config->{param}->{endMax}->[0] =~ /^\d+?/) {
    $ucscEnd    =   $config->{param}->{endMax}->[0] }    

  if ($ucscEnd < $ucscStart) {
    $ucscEnd    =   $ucscStart }

  $exporter->{handover_types}->{UCSClink}->{link_post}  =   '&ucscChro='.$config->{param}->{referenceName}->[0].'&ucscStart='.$ucscStart.'&ucscEnd='.$ucscEnd;

  $exporter->create_handover_exporter();
  
  $varResp		  =		$prefetch->{handover}->{'variants::digest'}->{target_values};

	##############################################################################

  if ($prefetch->{counts}->{variants_match_count} > 0) { $counts->{'exists'} = \1 }

  if ($prefetch->{counts}->{biosamples_base_count} > 0) {
    $counts->{frequency}  =   sprintf "%.4f",  $prefetch->{counts}->{biosamples_match_count} / $prefetch->{counts}->{biosamples_base_count} }

  if (! $counts->{"exists"}) { $handover = [] }

  my $dsResp    =   {
    datasetId   =>  $dataset,
    "exists"    =>  $counts->{"exists"},
    accessType	=>	'PUBLIC',
    error       =>  $prefetch->{error},
    frequency   =>  $counts->{frequency} * 1,
    variantCount    =>  $prefetch->{counts}->{variants_match_count} * 1,
    varResp    	=>  $varResp,
    callCount   =>  $prefetch->{counts}->{callsets_match_count} * 1,
    sampleCount =>  $prefetch->{counts}->{biosamples_match_count} * 1,
    note        =>  q{},
    externalUrl =>  $config->{url},
    datasetHandover =>  $exporter->{handover},
    info        =>  {
      description   =>  'The query was against database "'.$dataset.'", variant collection "'.$config->{collection_names}->{variant_collection}.'". '.$prefetch->{counts}->{callsets_match_count}.' matched callsets for '.$prefetch->{counts}->{variants_match_count}.' distinct variants. Out of '.$prefetch->{counts}->{biosamples_base_count}.' biosamples in the database, '.$prefetch->{counts}->{biosamples_query_match_count}.' matched the biosample query; of those, '.$prefetch->{counts}->{biosamples_match_count}.' had the variant.',
      distinctVarCount  =>  $prefetch->{handover}->{'variants::digest'}->{target_count},
    },
  };

  my $v_pars_conf =   $config->{q_conf}->{scopes}->{variants}->{parameters};
  foreach (keys %{ $v_pars_conf }) {
  #  $response->{alleleRequest}->{$_}  =   \0;
    if ($config->{param}->{$_}->[0] =~ /$v_pars_conf->{$_}->{pattern}/) {
      $dsResp->{$_}  =   $config->{param}->{$_}->[0];
      if ($v_pars_conf->{$_}->{type} eq 'integer') {
         $dsResp->{$_} *= 1 }
    }
  }

  return $dsResp;

}


1;
