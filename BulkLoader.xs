/* Win32::ODBC::BulkLoader implementation.
 *
 * Copyright 2009, Information Balance
 *
 */

#ifdef __cplusplus
extern "C" {
#endif
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#ifdef __cplusplus
}
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sql.h>
#include <sqlext.h>
#include <odbcss.h>

#define ERROR_BUFFER_SIZE 2048

void Cleanup(SQLHENV henv, HDBC hdbc1) {
   if (hdbc1 != SQL_NULL_HDBC) {
      SQLDisconnect(hdbc1);
      SQLFreeHandle(SQL_HANDLE_DBC, hdbc1);
   }

   if (henv != SQL_NULL_HENV) {
      SQLFreeHandle(SQL_HANDLE_ENV, henv);
   }
}

void get_error_message(char *msg, char *fn, SQLHANDLE handle, SQLSMALLINT type) {
    SQLINTEGER i = 0;
    SQLINTEGER native;
    SQLCHAR state[ 7 ];
    SQLCHAR text[256];
    SQLSMALLINT len;
    SQLRETURN ret;
    int size;
    int remaining = ERROR_BUFFER_SIZE;
    char *buffer = msg;
    
    size = snprintf(buffer, remaining, "Error calling %s: ", fn);
	if (size >= remaining) {
		return;
	}
	buffer += size;
	remaining -= size;
	if (handle == NULL) {
		return;
	}

    do {
    	ret = SQLGetDiagRec(type, handle, ++i, state, &native, text,
    	sizeof(text), &len );
    	if (SQL_SUCCEEDED(ret)) {
    		//printf("%s:%ld:%ld:%s\n", state, i, native, text);
    		// -122 is a status indicator indicating the number of rows, so we can skip it
    		if (native == -122) {
    			continue;
    		}
    		size = snprintf(buffer, remaining, "%s:%ld:%ld:%s\n", state, i, native, text);
    		if (size >= remaining) {
				return;
			}
			buffer += size;
			remaining -= size;
    	}
    } while( ret == SQL_SUCCESS );
}

int _bulk_load(const char *cDSN, const char *cTable, const char *cDataFile, const char *cFormatFile, int empty_is_default) {

   SQLHENV henv = SQL_NULL_HENV;
   HDBC hdbc1 = SQL_NULL_HDBC; 
   char errmsg[ERROR_BUFFER_SIZE];

   RETCODE retcode;
   SDWORD cRows;

   // Allocate the ODBC environment and save handle.
   //printf("Allocating handle 1.\n");
   retcode = SQLAllocHandle (SQL_HANDLE_ENV, NULL, &henv);
   if ( (retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS)) {
      get_error_message(errmsg, "SQLAllocHandle", NULL, 0);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }

   // Notify ODBC that this is an ODBC 3.0 app.
   //printf("Notify ODBC\n");
   retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_INTEGER);
   if ( (retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS)) {
      get_error_message(errmsg, "SQLSetEnvAttr", NULL, 0);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }

   // Allocate ODBC connection handle, set BCP mode, and connect.
   //printf("Allocating handle 2.\n");
   retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc1);
   if ( (retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS)) {
      get_error_message(errmsg, "SQLAllocHandle", NULL, 0);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }

   //printf("Setting connect attributes\n");
   retcode = SQLSetConnectAttr(hdbc1, SQL_COPT_SS_BCP, (void *)SQL_BCP_ON, SQL_IS_INTEGER);
   if ( (retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS)) {
      get_error_message(errmsg, "SQLSetConnectAttr", NULL, 0);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }

   // Sample uses Integrated Security. Create SQL Server DSN using Windows NT authentication.
   //printf("Connecting\n");
   retcode = SQLDriverConnect(hdbc1, NULL, (UCHAR*)cDSN, SQL_NTS,
                       NULL, 0, NULL, SQL_DRIVER_COMPLETE);
   if ( (retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO) ) {
      get_error_message(errmsg, "SQLDriverConnect", hdbc1, SQL_HANDLE_DBC);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }

   // Initialize the bulk copy.
   //printf("Initializing\n");
   //char *cErrorFile = "C:/errors.txt";
   //printf("Connecting to table: %s, file: %s\n", cTable, cDataFile);
   retcode = bcp_init(hdbc1, cTable, cDataFile, NULL, DB_IN);
   if ( (retcode != SUCCEED) ) {
      get_error_message(errmsg, "bcp_init", hdbc1, SQL_HANDLE_DBC);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }
   
   // Read the format file.
   if (cFormatFile != NULL) {
     //printf("Reading format file: %s\n", cFormatFile);
     retcode = bcp_readfmt(hdbc1, cFormatFile);
     if ( (retcode != SUCCEED) ) {
        get_error_message(errmsg, "bcp_readfmt", hdbc1, SQL_HANDLE_DBC);
        Cleanup(henv, hdbc1);
        croak(errmsg);
        return;
     }
   }

   //printf("Configuring KEEPIDENTITY\n");
   retcode = bcp_control(hdbc1, BCPKEEPIDENTITY, (void*) TRUE);
   if ( (retcode != SUCCEED) ) {
      get_error_message(errmsg, "bcp_control", hdbc1, SQL_HANDLE_DBC);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }
   
   //printf("Configuring TABXLOCK\n");
   char *cHint = "TABLOCK";
   retcode = bcp_control(hdbc1, BCPHINTS, (void*) cHint);
   if ( (retcode != SUCCEED) ) {
      get_error_message(errmsg, "bcp_control", hdbc1, SQL_HANDLE_DBC);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }

   //printf("empty_is_default = %d\n", empty_is_default);
   if (empty_is_default == 0) {
      //printf("Configuring KEEPNULLS\n");
      retcode = bcp_control(hdbc1, BCPKEEPNULLS, (void*) TRUE);
      if ( (retcode != SUCCEED) ) {
         get_error_message(errmsg, "bcp_control", hdbc1, SQL_HANDLE_DBC);
         Cleanup(henv, hdbc1);
         croak(errmsg);
         return;
      }
   } else {
      //printf("NOT Configuring KEEPNULLS\n");
   }

   // Execute the bulk copy.
   //printf("Executing\n");
   retcode = bcp_exec(hdbc1, &cRows);
   //printf("Executed: result is %d\n", retcode);
   if ( (retcode != SUCCEED) ) {
      get_error_message(errmsg, "bcp_exec", hdbc1, SQL_HANDLE_DBC);
      Cleanup(henv, hdbc1);
      croak(errmsg);
      return;
   }

   //printf("Done: result is %d rows\n", cRows);
   // Cleanup
   SQLDisconnect(hdbc1);
   SQLFreeHandle(SQL_HANDLE_DBC, hdbc1);
   SQLFreeHandle(SQL_HANDLE_ENV, henv);
   
   return cRows;
}

MODULE = Win32::ODBC::BulkLoader   PACKAGE = Win32::ODBC::BulkLoader

U32
load (dsn, table, file, formatFile, empty_is_default)
		const char *dsn;
		const char *table;
		const char *file;
		const char *formatFile;
		int empty_is_default;
    CODE:
        RETVAL = _bulk_load(dsn, table, file, formatFile, empty_is_default);
    OUTPUT:
        RETVAL

