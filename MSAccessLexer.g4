/*
* MSAccessLexer
* Based on SQLiteLexer grammar: 
* https://github.com/antlr/grammars-v4/blob/master/sql/sqlite/SQLiteLexer.g4
*/
// $antlr-format alignTrailingComments on, columnLimit 150, maxEmptyLinesToKeep 1, reflowComments off, useTab off
// $antlr-format allowShortRulesOnASingleLine on, alignSemicolons ownLine

lexer grammar MSAccessLexer;

options { caseInsensitive = true; }

SCOL:      ';';
DOT:       '.';
OPEN_PAR:  '(';
CLOSE_PAR: ')';
OPEN_BRACKET:  '[';
CLOSE_BRACKET: ']';
COMMA:     ',';
STAR:      '*';
PLUS:      '+';
MINUS:     '-';
PIPE2:     '||';
DIV:       '/';
IDIV:      '\\';
MOD_:      'MOD';
LT2:       '<<';
GT2:       '>>';
AMP:       '&';
PIPE:      '|';
LT:        '<';
LT_EQ:     '<=';
GT:        '>';
GT_EQ:     '>=';
EQ:        '=';
NOT_EQ1:   '!=';
NOT_EQ2:   '<>';

// http://www.sqlite.org/lang_keywords.html
// modified for: https://learn.microsoft.com/en-us/office/client-developer/access/reserved-words-access-custom-web-app
ADD_:               'ADD';
ALL_:               'ALL';
ALTER_:             'ALTER';
AND_:               'AND';
ANY_:               'ANY';
AS_:                'AS';
ASC_:               'ASC';
BEGIN_:             'BEGIN';
BETWEEN_:           'BETWEEN';
BY_:                'BY';
CASCADE_:           'CASCADE';
CASE_:              'CASE';
CAST_:              'CAST';
CHECK_:             'CHECK';
COLUMN_:            'COLUMN';
COMMIT_:            'COMMIT';
COMP_:              'COMP';
COMPRESSION_:       'COMPRESSION';
CONSTRAINT_:        'CONSTRAINT';
CREATE_:            'CREATE';
CURRENT_DATE_:      'CURRENT_DATE';
CURRENT_TIME_:      'CURRENT_TIME';
CURRENT_TIMESTAMP_: 'CURRENT_TIMESTAMP';
DATABASE_:          'DATABASE';
DEFAULT_:           'DEFAULT';
DELETE_:            'DELETE';
DESC_:              'DESC';
DISALLOW_:          'DISALLOW';
DISTINCT_:          'DISTINCT';
DISTINCTROW_:       'DISTINCTROW';
DROP_:              'DROP';
ELSE_:              'ELSE';
END_:               'END';
EQV_:               'EQV';
ESCAPE_:            'ESCAPE';
EXCEPT_:            'EXCEPT';
EXEC_:              'EXEC';
EXISTS_:            'EXISTS';
FOREIGN_:           'FOREIGN';
FROM_:              'FROM';
GROUP_:             'GROUP';
HAVING_:            'HAVING';
IGNORE_:            'IGNORE';
IN_:                'IN';
INDEX_:             'INDEX';
INDEXED_:           'INDEXED';
INNER_:             'INNER';
INSERT_:            'INSERT';
INTERSECT_:         'INTERSECT';
INTO_:              'INTO';
IS_:                'IS';
JOIN_:              'JOIN';
KEY_:               'KEY';
LEFT_:              'LEFT';
ALIKE_:             'ALIKE';
LIKE_:              'LIKE';
TOP_:               'TOP';
MATCH_:             'MATCH';
MINUS_:             'MINUS';
NO_:                'NO';
NOT_:               'NOT';
NULL_:              'NULL';
ON_:                'ON';
OR_:                'OR';
ORDER_:             'ORDER';
OUTER_:             'OUTER';
PARAMETERS_:        'PARAMETERS';
PASSWORD_:          'PASSWORD';
PRIMARY_:           'PRIMARY';
PROC_:              'PROC';
PROCEDURE_:         'PROCEDURE';
REFERENCES_:        'REFERENCES';
REGEXP_:            'REGEXP';
RIGHT_:             'RIGHT';
ROLLBACK_:          'ROLLBACK';
SELECT_:            'SELECT';
SET_:               'SET';
SOME_:              'SOME';
TABLE_:             'TABLE';
TEMP_:              'TEMP';
TEMPORARY_:         'TEMPORARY';
THEN_:              'THEN';
TO_:                'TO';
TRANSACTION_:       'TRANSACTION';
USER_:              'USER';
WORK_:              'WORK';
UNION_:             'UNION';
UNIQUE_:            'UNIQUE';
UPDATE_:            'UPDATE';
VALUES_:            'VALUES';
VIEW_:              'VIEW';
WHEN_:              'WHEN';
WHERE_:             'WHERE';
WITH_:              'WITH';
XOR_:               'XOR';
TRUE_:              'TRUE';
FALSE_:             'FALSE';
NULLS_:             'NULLS';
FIRST_:             'FIRST';
LAST_:              'LAST';
OBJECT_:            'OBJECT';
CONTAINER_:         'CONTAINER';
GRANT_:             'GRANT';
REVOKE_:            'REVOKE';
SELECTSECURITY_:    'SELECTSECURITY';
UPDATESECURITY_:    'UPDATESECURITY';
DBPASSWORD_:        'DBPASSWORD';
UPDATEIDENTITY_:    'UPDATEIDENTITY';
SELECTSCHEMA_:      'SELECTSCHEMA';
SCHEMA_:            'SCHEMA';
UPDATEOWNER_:       'UPDATEOWNER';
LONGBINARY_:        'LONGBINARY';
BINARY_:            'BINARY';
BIT_:               'BIT';
COUNTER_:           'COUNTER';
CURRENCY_:          'CURRENCY';
DATE_:              'DATE';
TIME_:              'TIME';
DATETIME_:          'DATETIME';
TIMESTAMP_:         'TIMESTAMP';
GUID_:              'GUID';
LONGTEXT_:          'LONGTEXT';
SINGLE_:            'SINGLE';
DOUBLE_:            'DOUBLE';
UNSIGNED_:          'UNSIGNED';
BYTE_:              'BYTE';
SHORT_:             'SHORT';
INTEGER_:           'INTEGER';
LONG_:              'LONG';
NUMERIC_:           'NUMERIC';
DECIMAL_:           'DECIMAL';
IDENTITY_:          'IDENTITY';
AUTOINCREMENT_:     'AUTOINCREMENT';
VARCHAR_:           'VARCHAR';
VARBINARY_:         'VARBINARY';
YESNO_:             'YESNO';
TEXT_:              'TEXT';
REPLICATIONID_:     'REPLICATIONID';
AUTONUMBER_:        'AUTONUMBER';
OLEOBJECT_:         'OLEOBJECT';
MEMO_:              'MEMO';
HYPERLINK_:         'HYPERLINK';
PERCENT_:           'PERCENT';


IDENTIFIER:
    '"' (~'"' | '""')* '"'
    | '`' (~'`' | '``')* '`'
    | '[' ~']'* ']'
    | [\u00AA\u00B5\u00BA\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u00FFA-ZÕÄÖÜ_] [\u00AA\u00B5\u00BA\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u00FFA-ZÕÄÖÜ_0-9]*
; // TODO check: needs more chars in set

NUMERIC_LITERAL: ((MINUS)?(DIGIT+ ('.' DIGIT*)?) | ('.' DIGIT+)) ('E' [-+]? DIGIT+)? | '0x' HEX_DIGIT+;

BIND_PARAMETER: '?' DIGIT* | [:@$] IDENTIFIER;

STRING_LITERAL: '\'' ( ~'\'' | '\'\'')* '\'';

DATE_LITERAL: '#' ( ~'#' )* '#';

BLOB_LITERAL: 'X' STRING_LITERAL;

SINGLE_LINE_COMMENT: '--' ~[\r\n]* (('\r'? '\n') | EOF) -> channel(HIDDEN);

MULTILINE_COMMENT: '/*' .*? '*/' -> channel(HIDDEN);

SPACES: [ \u000B\t\r\n\u000C\u00A0] -> channel(HIDDEN);  // More whitespace characters, such as NBSP

UNEXPECTED_CHAR: .;

fragment HEX_DIGIT: [0-9A-F];
fragment DIGIT:     [0-9];