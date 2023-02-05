/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/file.h>
#include <stdbool.h>
#include <dirent.h>

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL			16
#define MAX_TOK_LEN			32
#define KEYWORD_OFFSET	10
#define STRING_BREAK		" (),<>="
#define NUMBER_BREAK		" ),"

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN+4];
	int				num_columns;
	int				cd_offset;
	int       tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int				list_size;
	int				num_tables;
	int				db_flags;
	tpd_entry	tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked
	 together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

typedef struct table_file_header_def
{
	int			file_size;			// 4 bytes
	int			record_size;			// 4 bytes
	int			num_records;			// 4 bytes
	int			record_offset;			// 4 bytes
	int			file_header_flag;		// 4 bytes
	tpd_entry		*tpd_ptr;			// 8 bytes. (e.g. 64-bit pointer)
} table_file_header;					// minimum size = 28

/* for double joins */
typedef struct record_pair_def {
  char *tab_a_rec;
  char *tab_b_rec;
} record_pair;

/* This enum defines the different classes of tokens for
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
  function_name,// 6
	terminator,		// 7
	error			    // 8

} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_CHAR,		    // 11
	T_VARCHAR,		    // 12
	K_CREATE, 		// 13
	K_TABLE,			// 14
	K_NOT,				// 15
	K_NULL,				// 16
	K_DROP,				// 17
	K_LIST,				// 18
	K_SCHEMA,			// 19
  K_FOR,        // 20
	K_TO,				  // 21
  K_INSERT,     // 22
  K_INTO,       // 23
  K_VALUES,     // 24
  K_DELETE,     // 25
  K_FROM,       // 26
  K_WHERE,      // 27
  K_UPDATE,     // 28
  K_SET,        // 29
  K_SELECT,     // 30
  K_ORDER,      // 31
  K_BY,         // 32
  K_DESC,       // 33
  K_IS,         // 34
  K_AND,        // 35
  K_OR,         // 36 - new keyword should be added below this line
  K_NATURAL,    // 37
  K_JOIN,       // 38
  K_GROUP,      // 39
  F_SUM,        // 40
  F_AVG,        // 41
	F_COUNT,      // 42 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN,		  // 71
	S_COMMA,			      // 72
  S_STAR,             // 73
  S_EQUAL,            // 74
  S_LESS,             // 75
  S_GREATER,          // 76
	IDENT = 85,			    // 85
	INT_LITERAL = 90,	  // 90
  STRING_LITERAL,     // 91
	EOC = 95,			      // 95
	INVALID = 99		    // 99
} token_value;

/* This constants must be updated when add new keywords */ // Earlier 30
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 33

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
const char *keyword_table[] =
{
  "int", "char", "varchar", "create", "table", "not", "null", "drop", "list", "schema",
  "for", "to", "insert", "into", "values", "delete", "from", "where",
  "update", "set", "select", "order", "by", "desc", "is", "and", "or", "natural", "join", "group",
  "sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
  INVALID_STATEMENT = -199,	// -199
  CREATE_TABLE = 100,				// 100
  DROP_TABLE,								// 101
  LIST_TABLE,								// 102
  LIST_SCHEMA,							// 103
  INSERT,                   // 104
  DELETE,                   // 105
  UPDATE,                   // 106
  SELECT                    // 107
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME,				// -398
	TABLE_NOT_EXIST,						// -397
	INVALID_TABLE_DEFINITION,		// -396
	INVALID_COLUMN_NAME,				// -395
	DUPLICATE_COLUMN_NAME,			// -394
	COLUMN_NOT_EXIST,						// -393
	MAX_COLUMN_EXCEEDED,				// -392
	INVALID_TYPE_NAME,					// -391
	INVALID_COLUMN_DEFINITION,	// -390
	INVALID_COLUMN_LENGTH,			// -389
  INVALID_REPORT_FILE_NAME,		// -388
  /* Must add all the possible errors from I/U/D + SELECT here */
	FILE_OPEN_ERROR = -299,			// -299
	DBFILE_CORRUPTION,					// -298
	MEMORY_ERROR,							  // -297
	INVALID_INSERT_STATEMENT,         // -296
	INVALID_COMMON_JOIN               // -295
} return_codes;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
bool compare_records_by_val(const char *record_a, const char *record_b,
                            cd_entry *order_cd, int field_offset, bool desc);
void add_to_list(token_list **tok_list, const char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int sem_insert_table(token_list *t_list);
int sem_select_table(token_list *t_list);
int sem_select_aggregator(token_list *t_list);
int sem_select_agg_nj(token_list *t_list);
int sem_select_natural_join(int cols_print_ct, token_list *first_col_tok, token_list *first_tab_token,token_list *second_tab_token);
int sem_select_natural_join_agg(token_list *agg_token, int cols_print_ct, token_list *first_col_tok, token_list *first_tab_token,token_list *second_tab_token);
int sem_update_data(token_list *t_list);
int sem_delete_data(token_list *t_list);
int delete_table_file(char *tab_name);
table_file_header *get_file_header(char *tab_name);
int create_table_data_file(char *tab_name, table_file_header *table_file_header, int size);
cd_entry *get_cd(char *table_name, char *col_name);
int get_print_size(cd_entry *cd);
void *double_switch(int swtch, void *return_zero, void *return_one);
bool cmp_rec_pairs(record_pair a, record_pair b, cd_entry *order_cd, int which, int field_offset, bool desc);
bool chk_satisfies_condition(char *field, int operator_type, token_list *comp_value_token, int col_len);
int get_compare_values(token_list *cur_token, char *table_name, cd_entry *first_cd, bool is_join,
                     int *r_comp_type, token_list **r_comp_value_token, cd_entry **r_compare_cd,
                     int *r_comp_field_offset);

/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);