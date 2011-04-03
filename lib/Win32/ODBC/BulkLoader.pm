package Win32::ODBC::BulkLoader;
use warnings;
use strict;

our $VERSION = '0.01';

require Exporter;
require DynaLoader;
our @ISA = qw( Exporter DynaLoader );
our @EXPORT_OK = qw( load );
our %EXPORT_TAGS = ( all => \@EXPORT_OK );

bootstrap Win32::ODBC::BulkLoader;

1;

__END__


=head1 NAME

Win32::ODBC::BulkLoader - bulk load through ODBC

=head1 SYNOPSIS

    use Win32::ODBC::BulkLoader qw(load);

    my $result = load(...);

=head1 DESCRIPTION

This module can be used to bulk load data through ODBC. It would be lovely
if this was part of DBI, but it isn't. 

=head1 FUNCTIONS

=over 4

=item load($dsn, $table, $file, $formatFile, $empty_is_default);

This function uses the ODBC bulk copying API to load tab-delimited formatted
data into an ODBC-accessible database table. The chances are you do not need
this function, as it is really only important if you need to bulk load over
a network. Local tab files can usually be loaded by SQL directly. 

The parameters are the ODBC DSN, the table name, the file name, a file 
reference to the format file used in bulk loading, and a single boolean
flag which affects the handling of empty values. In a bulk load, an empty
value can either be the empty string, or a request to use the table's default
value for that column. Yes, it would be nice if there was a way of specifying
a null value in bulkloading, but in current versions of ODBC that seems to be
impossible. Even so, this module is handy when you are running a distributed
system or need to bulk load to a remote DB server. 

The return value is the number of rows successfully loaded. 

=back

=head1 AUTHOR

Author: Stuart Watt E<lt>swatt@infobal.comE<gt>

=cut

