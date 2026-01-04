/*
* MSAccessParser
* Based on SQLiteParser grammar: 
* https://github.com/antlr/grammars-v4/blob/master/sql/sqlite/SQLiteParser.g4
*/
// $antlr-format alignTrailingComments on, columnLimit 130, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments off
// $antlr-format useTab off, allowShortRulesOnASingleLine off, allowShortBlocksOnASingleLine on, alignSemicolons ownLine

parser grammar MSAccessParser;

options {
    tokenVocab = MSAccessLexer;
}

keyword:
    ADD_
    | ALL_
    | ALTER_
    | AND_
    | AS_
    | ASC_
    | BEGIN_
    | BETWEEN_
    | BY_
    | CASCADE_
    | CASE_
    | CAST_
    | CHECK_
    | COLUMN_
    | COMMIT_
    | COMP_
    | COMPRESSION_
    | CONSTRAINT_
    | CONTAINER_
    | CREATE_
    | CURRENT_DATE_
    | CURRENT_TIME_
    | CURRENT_TIMESTAMP_
    | DATABASE_
    | DEFAULT_
    | DELETE_
    | DESC_
    | DISALLOW_
    | DISTINCT_
    | DISTINCTROW_
    | DROP_
    | ELSE_
    | END_
    | EQV_
    | ESCAPE_
    | EXCEPT_
    | EXISTS_
    | FOREIGN_
    | FROM_
    | GRANT_
    | GROUP_
    | HAVING_
    | IGNORE_
    | IN_
    | INDEX_
    | INDEXED_
    | INNER_
    | INSERT_
    | INTERSECT_
    | INTO_
    | IS_
    | JOIN_
    | KEY_
    | LEFT_
    | ALIKE_
    | LIKE_
    | TOP_
    | MATCH_
    | MINUS_
    | NO_
    | NOT_
    | NULL_
    | OBJECT_
    | ON_
    | OR_
    | ORDER_
    | OUTER_
    | PARAMETERS_
    | PASSWORD_
    | PRIMARY_
    | PROCEDURE_
    | REFERENCES_
    | REGEXP_
    | REVOKE_
    | RIGHT_
    | ROLLBACK_
    | SELECT_
    | SET_
    | TABLE_
    | TEMP_
    | TEMPORARY_
    | THEN_
    | TO_
    | TRANSACTION_
    | USER_
    | WORK_
    | UNION_
    | UNIQUE_
    | UPDATE_
    | VALUES_
    | VIEW_
    | WHEN_
    | WHERE_
    | WITH_
    | XOR_
    | TRUE_
    | FALSE_
    | NULLS_
    | FIRST_
    | LAST_
    | SELECTSECURITY_
    | UPDATESECURITY_
    | DBPASSWORD_
    | UPDATEIDENTITY_
    | SELECTSCHEMA_
    | SCHEMA_
    | UPDATEOWNER_
    | LONGBINARY_
    | BINARY_
    | BIT_
    | COUNTER_
    | CURRENCY_
    | DATE_
    | TIME_
    | DATETIME_
    | TIMESTAMP_
    | GUID_
    | LONGTEXT_
    | SINGLE_
    | DOUBLE_
    | UNSIGNED_
    | BYTE_
    | SHORT_
    | INTEGER_
    | LONG_
    | NUMERIC_
    | DECIMAL_
    | IDENTITY_
    | AUTOINCREMENT_
    | VARCHAR_
    | VARBINARY_
    | YESNO_
    | TEXT_
    | REPLICATIONID_
    | AUTONUMBER_
    | OLEOBJECT_
    | MEMO_
    | HYPERLINK_
    | PERCENT_
;
privilege:
    SELECT_
    | DELETE_
    | INSERT_
    | UPDATE_
    | CREATE_
    | DROP_
    | SELECTSECURITY_
    | UPDATESECURITY_
    | DBPASSWORD_
    | UPDATEIDENTITY_
    | SELECTSCHEMA_
    | SCHEMA_
    | UPDATEOWNER_
;
type_name:
    LONGBINARY_
    | BINARY_
    | BIT_
    | COUNTER_
    | CURRENCY_
    | DATE_
    | TIME_
    | DATETIME_
    | TIMESTAMP_
    | GUID_
    | LONGTEXT_
    | SINGLE_
    | DOUBLE_
    | UNSIGNED_ BYTE_
    | SHORT_
    | INTEGER_
    | LONG_
    | NUMERIC_
    | DECIMAL_
    | IDENTITY_
    | AUTOINCREMENT_
    | VARCHAR_
    | VARBINARY_
    | YESNO_
    | TEXT_
    | REPLICATIONID_
    | AUTONUMBER_
    | OLEOBJECT_
    | MEMO_
    | HYPERLINK_
;
literal_expr:
    literal=(NUMERIC_LITERAL
    | STRING_LITERAL
    | DATE_LITERAL
    | BLOB_LITERAL
    | NULL_
    | TRUE_
    | FALSE_
    | CURRENT_TIME_
    | CURRENT_DATE_
    | CURRENT_TIMESTAMP_)
;
column_alias:         IDENTIFIER | STRING_LITERAL;
any_name:             IDENTIFIER | keyword | STRING_LITERAL | OPEN_PAR any_name CLOSE_PAR;
name:                 any_name;
function_name:        any_name;
table_name:           any_name;
column_name:          any_name;
index_name:           any_name;
procedure_name:       any_name;
param_name:           any_name;
view_name:            any_name;
table_alias:          any_name;
aliased_table_name:   table_name (AS_? table_alias)?;
direction:            ASC_ | DESC_;
ordering_term:        orderingExpr=expr orderingDirection=(ASC_ | DESC_)? (NULLS_ (FIRST_ | LAST_))?;
signed_number:        (PLUS | MINUS)? NUMERIC_LITERAL;
type_params:          OPEN_PAR NUMERIC_LITERAL (COMMA NUMERIC_LITERAL)? CLOSE_PAR;  // Allows for things like VARCHAR(255) or DECIMAL(10,2)
param_def:            param_name type_name type_params?;
optional_parens:      OPEN_PAR CLOSE_PAR;
default_expr:         DEFAULT_ (signed_number | literal_expr | OPEN_PAR expr CLOSE_PAR | IDENTIFIER optional_parens?);
identity_spec:        (IDENTITY_ | AUTOINCREMENT_) type_params?;
column_def:           param_def identity_spec? (NOT_ NULL_)? (WITH_ COMPRESSION_ | COMP_)? default_expr? single_field_constraint?;
prefixed_star:        table_name DOT STAR;
prefixed_column_name: (prefixName=table_name DOT)* columnName=column_name;
result_column:        STAR | prefixed_star | columnExpr=expr (AS_ columnAlias=column_alias)?;
param_expr:           prefixed_star | DISTINCT_? expr;
user_name:            IDENTIFIER;
group_name:           IDENTIFIER;
user_or_group_name:   IDENTIFIER;
password:             STRING_LITERAL;
pid:                  NUMERIC_LITERAL;

/*
    https://support.microsoft.com/en-us/office/table-of-operators-e1bc04d5-8b76-429f-a252-e9223117d6bd
    arithmetic: + - * / \ mod ^
    comparison: < <= > >= = <> !=
    logical: and or eqv not xor
    concatenation: & +
    special: is like between in
 */
expr:
    literalExpr=literal_expr
    | bindParameter=BIND_PARAMETER
    | prefixedColumnName=prefixed_column_name
    | op=(MINUS | PLUS | NOT_) rhs=expr /* might be boolean */
    | functionExpr=function_expr /* might be boolean? */
    | op=CAST_ OPEN_PAR lhs=expr AS_ typeName=type_name CLOSE_PAR
    | lhs=expr op=AMP rhs=expr
    | lhs=expr op=(STAR | DIV | IDIV | MOD_) rhs=expr
    | lhs=expr op=(PLUS | MINUS) rhs=expr
    | lhs=expr op=(EQ | NOT_EQ1 | NOT_EQ2 | LT | LT_EQ | GT | GT_EQ) rhs=expr /* boolean */
    | lhs=expr op=(EQ | NOT_EQ1 | NOT_EQ2 | LT | LT_EQ | GT | GT_EQ) selector=(ALL_ | ANY_ | SOME_) OPEN_PAR subquery=select_stmt CLOSE_PAR /* boolean */
    | lhs=expr op=IS_ inv=NOT_? rhs=expr  /* boolean */
    | lhs=expr inv=NOT_? op=IN_ OPEN_PAR (subquery=select_stmt | expressions+=expr (COMMA expressions+=expr)*) CLOSE_PAR /* boolean */
    | lhs=expr inv=NOT_? op=(LIKE_ | ALIKE_ | REGEXP_ | MATCH_) rhs=expr (ESCAPE_ escape=expr)?  /* boolean */
    | lhs=expr inv=NOT_? op=BETWEEN_ start=expr AND_ stop=expr  /* boolean */
    | lhs=expr op=XOR_ rhs=expr /* boolean */
    | lhs=expr op=AND_ rhs=expr /* boolean */
    | lhs=expr op=OR_ rhs=expr /* boolean */
    | lhs=expr op=EQV_ rhs=expr /* boolean */
    | inv=NOT_? op=EXISTS_ OPEN_PAR subquery=select_stmt CLOSE_PAR /* boolean */
    | op=CASE_ caseExpr=expr? (WHEN_ whenExpr+=expr THEN_ thenExpr+=expr)+ (ELSE_ elseExpr=expr)? END_
    | OPEN_PAR (subquery=select_stmt | expr) CLOSE_PAR
;

function_expr:
    functionName=function_name OPEN_PAR ((params+=param_expr ( COMMA params+=param_expr)*) | STAR)? CLOSE_PAR
;

create_user_stmt:
    CREATE_ USER_ user_name password pid (COMMA user_name password pid)*
;

create_group_stmt:
    CREATE_ GROUP_ group_name pid (COMMA group_name pid)*
;

drop_user_stmt:
    DROP_ USER_ user_name (COMMA user_name)* (FROM_ group_name)?
;

drop_group_stmt:
    DROP_ GROUP_ group_name (COMMA group_name)*
;

alter_password_stmt:
    ALTER_ (DATABASE_ | (USER_ user_name)) PASSWORD_ password password
;

add_user_stmt:
    ADD_ USER_ user_name (COMMA user_name)* TO_ group_name
;

grant_stmt:
    GRANT_ privilege (COMMA privilege)* ON_ object=(TABLE_ | OBJECT_ | CONTAINER_) any_name TO_ user_or_group_name (COMMA user_or_group_name)*
;

revoke_stmt:
    REVOKE_ privilege (COMMA privilege) ON_ object=(TABLE_ | OBJECT_ | CONTAINER_) any_name FROM_ user_or_group_name (COMMA user_or_group_name)*
;

alter_table_stmt:
    ALTER_ TABLE_ table_name (ADD_ (COLUMN_ param_def | multiple_field_constraint) | DROP_ (COLUMN_ column_name | CONSTRAINT_ index_name) | ALTER_ (COLUMN_ param_def | multiple_field_constraint))
;

begin_stmt:
    BEGIN_ (TRANSACTION_)?
;

commit_stmt:
    COMMIT_ (TRANSACTION_ | WORK_)?
;

rollback_stmt:
    ROLLBACK_ (TRANSACTION_ | WORK_)?
;

on_trigger:
    ON_ (UPDATE_ | DELETE_) (CASCADE_ | SET_ NULL_)
;

single_field_constraint:
    PRIMARY_ KEY_
    | UNIQUE_
    | REFERENCES_ table_name (OPEN_PAR column_name CLOSE_PAR)? on_trigger*
    | CHECK_ OPEN_PAR expr CLOSE_PAR
;

multiple_field_constraint:
    CONSTRAINT_ name (
        PRIMARY_ KEY_ (OPEN_PAR column_name (COMMA column_name)* CLOSE_PAR)?
        | UNIQUE_ (OPEN_PAR column_name (COMMA column_name)* CLOSE_PAR)?
        | FOREIGN_ KEY_ (NO_ INDEX_)? OPEN_PAR column_name (COMMA column_name)* CLOSE_PAR REFERENCES_ table_name (OPEN_PAR column_name (COMMA column_name)* CLOSE_PAR)? on_trigger*
        | CHECK_ OPEN_PAR expr CLOSE_PAR
    )
;

exec_stmt:
    EXEC_ any_name
;

create_table_stmt:
    CREATE_ (TEMPORARY_ | TEMP_)? TABLE_ table_name (OPEN_PAR column_def (COMMA column_def)* (COMMA multiple_field_constraint)* CLOSE_PAR | AS_ select_stmt)
;

create_view_stmt:
    CREATE_ VIEW_ view_name (OPEN_PAR column_name (COMMA column_name)* CLOSE_PAR)? AS_ select_stmt
;

create_index_stmt:
    CREATE_ UNIQUE_? INDEX_ index_name ON_ table_name OPEN_PAR column_name direction? (COMMA column_name direction?)* CLOSE_PAR (WITH_ (PRIMARY_ | DISALLOW_ NULL_ | IGNORE_ NULL_))?
;

create_procedure_stmt:
    CREATE_ (PROC_ | PROCEDURE_) procedure_name (OPEN_PAR param_def (COMMA param_def)* CLOSE_PAR)? AS_ (select_stmt | update_stmt | delete_stmt | insert_stmt | create_table_stmt | drop_stmt)
;

drop_stmt:
    DROP_ (object=(VIEW_ | PROCEDURE_) any_name | object=TABLE_ table_name CASCADE_? | object=INDEX_ index_name ON_ table_name)
;

select_stmt:
    statements+=select_core_stmt ((UNION_ | INTERSECT_ | EXCEPT_ | MINUS_) (DISTINCT_ | ALL_)? statements+=select_core_stmt)*
;

table_stmt:
    statements+=table_core_stmt ((UNION_ | INTERSECT_ | EXCEPT_ | MINUS_) (DISTINCT_ | ALL_)? statements+=table_core_stmt)*
;

table_core_stmt:
    TABLE_ tables+=table_name (COMMA tables+=table_name)*
;

select_clause:
    SELECT_ distinct=(DISTINCT_ | DISTINCTROW_ | ALL_)? 
    (top=TOP_ limit=NUMERIC_LITERAL percent=PERCENT_?)? 
    resultColumns+=result_column (COMMA resultColumns+=result_column)*
;

select_into_stmt:
    selectClause=select_clause INTO_ tableName=table_name 
    (
        fromClause=from_clause 
        joinClause+=join_clause* 
        whereClause=where_clause? 
        groupByClause=group_by_clause? 
        orderByClause=order_by_clause?
    )?
;

select_core_stmt:
    selectClause=select_clause (fromClause=from_clause joinClause+=join_clause* whereClause=where_clause? groupByClause=group_by_clause? orderByClause=order_by_clause?)?
;

table_or_subquery:
    (aliased_table_name (INDEXED_ BY_ index_name | NOT_ INDEXED_)?)
    | table_name OPEN_PAR expr (COMMA expr)* CLOSE_PAR (AS_? table_alias)?
    | OPEN_PAR (table_or_subquery (COMMA table_or_subquery)* join_clause*) CLOSE_PAR (AS_? table_alias)?
    | OPEN_PAR select_stmt CLOSE_PAR (AS_? table_alias)?
;

table_with_joins:
    aliased_table_name | OPEN_PAR (table_with_joins (COMMA table_with_joins)* join_clause*) CLOSE_PAR
;

from_clause:
    FROM_ tables+=table_or_subquery (COMMA tables+=table_or_subquery)* (where_clause)?
;

join_clause:
    ((LEFT_ | RIGHT_) OUTER_? | INNER_) JOIN_ table_or_subquery (on=ON_ expression=expr)?
;

where_clause:
    WHERE_ whereExpr=expr
;

group_by_clause:
    GROUP_ BY_ groupingTerms+=expr (COMMA groupingTerms+=expr)* (HAVING_ havingExpr=expr)?
;

order_by_clause:
    ORDER_ BY_ orderingTerms+=ordering_term (COMMA orderingTerms+=ordering_term)*
;

insert_stmt:
    INSERT_ INTO_ tableName=table_name (OPEN_PAR columnNames+=column_name (COMMA columnNames+=column_name)* CLOSE_PAR)? 
    (VALUES_ OPEN_PAR values+=expr (COMMA values+=expr)* CLOSE_PAR | subquery=select_core_stmt)
;

update_stmt:
    UPDATE_ table_with_joins (COMMA table_with_joins)* join_clause* SET_ prefixed_column_name EQ expr (COMMA prefixed_column_name EQ expr)* (WHERE_ where_expr=expr)?
;

delete_stmt:
    DELETE_ (DISTINCTROW_? (prefixed_star | STAR))? FROM_ table_with_joins (COMMA table_with_joins)* join_clause* (WHERE_ expr)?
;

procedure_stmt:
    PROCEDURE_ procedure_name (param_def (COMMA param_def)*)?
;

parameters_stmt:
    PARAMETERS_ param_def (COMMA param_def)*
;

sql_stmt: (
        alter_table_stmt
        | begin_stmt
        | commit_stmt
        | create_index_stmt
        | create_table_stmt
        | create_procedure_stmt
        | procedure_stmt
        | create_view_stmt
        | delete_stmt
        | drop_stmt
        | insert_stmt
        | rollback_stmt
        | select_stmt
        | table_stmt
        | select_into_stmt
        | update_stmt
        | parameters_stmt
        | create_user_stmt
        | create_group_stmt
        | drop_user_stmt
        | drop_group_stmt
        | alter_password_stmt
        | add_user_stmt
        | grant_stmt
        | revoke_stmt
        | exec_stmt
    )
;
sql_stmt_list: SCOL* sql_stmt (SCOL+ sql_stmt)* SCOL*;
parse: sql_stmt_list? EOF;
