/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

#ifdef _WINDOWS
#pragma warning(disable:4312) /* type corecing */
#endif

#include "ruby.h"
#include "cas_cci.h"
#include "stdlib.h"

#define MAX_STR_LEN     255

#define CUBRID_ER_INVALID_SQL_TYPE              -2002
#define CUBRID_ER_CANNOT_GET_COLUMN_INFO        -2003
#define CUBRID_ER_INIT_ARRAY_FAIL               -2004
#define CUBRID_ER_UNKNOWN_TYPE                  -2005
#define CUBRID_ER_INVALID_PARAM                 -2006
#define CUBRID_ER_INVALID_ARRAY_TYPE            -2007
#define CUBRID_ER_NOT_SUPPORTED_TYPE            -2008
#define CUBRID_ER_OPEN_FILE                     -2009
#define CUBRID_ER_CREATE_TEMP_FILE              -2010
#define CUBRID_ER_TRANSFER_FAIL                 -2011
#define CUBRID_ER_ALLOC_FAILED                  -2012


/* Maximum length for the Cubrid data types. 
 *
 * The max len of LOB is the max file size creatable in an external storage, 
 * so we ca't give the max len of LOB type, just use 1G. Please ignore it. 
 */
#define MAX_CUBRID_CHAR_LEN   1073741823
#define MAX_LEN_INTEGER	      (10 + 1)
#define MAX_LEN_SMALLINT      (5 + 1)
#define MAX_LEN_BIGINT	      (19 + 1)
#define MAX_LEN_FLOAT	      (14 + 1)
#define MAX_LEN_DOUBLE	      (28 + 1)
#define MAX_LEN_MONETARY      (28 + 2)
#define MAX_LEN_DATE	      10
#define MAX_LEN_TIME	      8
#define MAX_LEN_TIMESTAMP     23
#define MAX_LEN_DATETIME      MAX_LEN_TIMESTAMP
#define MAX_LEN_OBJECT	      MAX_CUBRID_CHAR_LEN
#define MAX_LEN_SET	      MAX_CUBRID_CHAR_LEN
#define MAX_LEN_MULTISET      MAX_CUBRID_CHAR_LEN
#define MAX_LEN_SEQUENCE      MAX_CUBRID_CHAR_LEN
#define MAX_LEN_LOB           MAX_CUBRID_CHAR_LEN

typedef struct {
  int    handle;
  char   host[MAX_STR_LEN];
  int    port;
  char   db[MAX_STR_LEN];
  char   user[MAX_STR_LEN];
  VALUE  auto_commit;
} Connection;

typedef struct {
  Connection       *con;
  int              handle;
  int              affected_rows;
  int              col_count;
  int              param_cnt;
  T_CCI_SQLX_CMD   sql_type;
  T_CCI_COL_INFO   *col_info;
  int              bound;
  T_CCI_BLOB       blob;
  T_CCI_BLOB       clob;
} Statement;

typedef struct {
  Connection       *con;
  char             oid_str[MAX_STR_LEN];
  int              col_count;
  VALUE            col_type;
  VALUE            hash;
} Oid;

#ifdef MS_WINDOWS
#define CUBRID_LONG_LONG _int64
#else
#define CUBRID_LONG_LONG long long
#endif

//ifndef CCI_ER_END
//#define CCI_ER_END (-20100)
//#endif


#ifndef RSTRING_PTR
#define RSTRING_PTR(str) RSTRING(str)->ptr
#endif

#ifndef RSTRING_LEN
#define RSTRING_LEN(str) RSTRING(str)->len
#endif

#ifndef HAVE_RB_STR_SET_LEN
#define rb_str_set_len(str, length) (RSTRING_LEN(str) = (length))
#endif

VALUE GeteCubrid();
extern void cubrid_handle_error(int e, T_CCI_ERROR *error);

#define GET_CONN_STRUCT(self, con) Data_Get_Struct((self), Connection, (con))
#define CHECK_CONNECTION(con, rtn) \
  do { \
    if (!((con)->handle)) { \
      cubrid_handle_error(CCI_ER_CON_HANDLE, NULL); \
      return (rtn); \
    } \
  } while(0)

#define GET_STMT_STRUCT(self, stmt) Data_Get_Struct((self), Statement, (stmt))
#define CHECK_HANDLE(stmt, rtn) \
  do { \
    if (!((stmt)->handle)) { \
      cubrid_handle_error(CCI_ER_REQ_HANDLE, NULL); \
      return (rtn); \
    } \
  } while(0)
  
typedef struct
{
    char *type_name;
    T_CCI_U_TYPE cubrid_u_type;
    int len;
} DB_TYPE_INFO;

/* Define Cubrid supported date types */
static const DB_TYPE_INFO db_type_info[] = {
    {"NULL", CCI_U_TYPE_NULL, 0},
    {"UNKNOWN", CCI_U_TYPE_UNKNOWN, MAX_LEN_OBJECT},

    {"CHAR", CCI_U_TYPE_CHAR, -1},
    {"STRING", CCI_U_TYPE_STRING, -1},
    {"NCHAR", CCI_U_TYPE_NCHAR, -1},
    {"VARNCHAR", CCI_U_TYPE_VARNCHAR, -1},

    {"BIT", CCI_U_TYPE_BIT, -1},
    {"VARBIT", CCI_U_TYPE_VARBIT, -1},

    {"NUMERIC", CCI_U_TYPE_NUMERIC, -1},
    {"NUMBER", CCI_U_TYPE_NUMERIC, -1},
    {"INT", CCI_U_TYPE_INT, MAX_LEN_INTEGER},
    {"SHORT", CCI_U_TYPE_SHORT, MAX_LEN_SMALLINT},
    {"BIGINT", CCI_U_TYPE_BIGINT, MAX_LEN_BIGINT},
    {"MONETARY", CCI_U_TYPE_MONETARY, MAX_LEN_MONETARY},

    {"FLOAT", CCI_U_TYPE_FLOAT, MAX_LEN_FLOAT},
    {"DOUBLE", CCI_U_TYPE_DOUBLE, MAX_LEN_DOUBLE},

    {"DATE", CCI_U_TYPE_DATE, MAX_LEN_DATE},
    {"TIME", CCI_U_TYPE_TIME, MAX_LEN_TIME},
    {"DATETIME", CCI_U_TYPE_DATETIME, MAX_LEN_DATETIME},
    {"TIMESTAMP", CCI_U_TYPE_TIMESTAMP, MAX_LEN_TIMESTAMP},

    {"SET", CCI_U_TYPE_SET, MAX_LEN_SET},
    {"MULTISET", CCI_U_TYPE_MULTISET, MAX_LEN_MULTISET},
    {"SEQUENCE", CCI_U_TYPE_SEQUENCE, MAX_LEN_SEQUENCE},
    {"RESULTSET", CCI_U_TYPE_RESULTSET, -1},

    {"OBJECT", CCI_U_TYPE_OBJECT, MAX_LEN_OBJECT},
    {"BLOB", CCI_U_TYPE_BLOB, MAX_LEN_LOB},
    {"CLOB", CCI_U_TYPE_CLOB, MAX_LEN_LOB},
    {"ENUM",CCI_U_TYPE_ENUM,-1}
};


