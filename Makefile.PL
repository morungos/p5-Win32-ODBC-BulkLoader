use ExtUtils::MakeMaker;

WriteMakefile(
    NAME            => 'Win32::ODBC::BulkLoader',
    AUTHOR          => 'Stuart Watt <swatt@infobal.com>',
    VERSION_FROM    => 'lib/Win32/ODBC/BulkLoader.pm',
    LIBS            => '-lodbcbcp.a'
);

