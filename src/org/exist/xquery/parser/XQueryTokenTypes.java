// $ANTLR 2.7.7 (2006-11-01): "XQuery.g" -> "XQueryParser.java"$

	package org.exist.xquery.parser;

	import antlr.debug.misc.*;
	import java.io.StringReader;
	import java.io.BufferedReader;
	import java.io.InputStreamReader;
	import java.util.ArrayList;
	import java.util.List;
	import java.util.Iterator;
	import java.util.Stack;
	import org.exist.storage.BrokerPool;
	import org.exist.storage.DBBroker;
	import org.exist.storage.analysis.Tokenizer;
	import org.exist.EXistException;
	import org.exist.dom.DocumentSet;
	import org.exist.dom.DocumentImpl;
	import org.exist.dom.QName;
	import org.exist.security.PermissionDeniedException;
	import org.exist.xquery.*;
	import org.exist.xquery.value.*;
	import org.exist.xquery.functions.fn.*;

public interface XQueryTokenTypes {
	int EOF = 1;
	int NULL_TREE_LOOKAHEAD = 3;
	int QNAME = 4;
	int PREDICATE = 5;
	int FLWOR = 6;
	int PARENTHESIZED = 7;
	int ABSOLUTE_SLASH = 8;
	int ABSOLUTE_DSLASH = 9;
	int WILDCARD = 10;
	int PREFIX_WILDCARD = 11;
	int FUNCTION = 12;
	int DYNAMIC_FCALL = 13;
	int UNARY_MINUS = 14;
	int UNARY_PLUS = 15;
	int XPOINTER = 16;
	int XPOINTER_ID = 17;
	int VARIABLE_REF = 18;
	int VARIABLE_BINDING = 19;
	int ELEMENT = 20;
	int ATTRIBUTE = 21;
	int ATTRIBUTE_CONTENT = 22;
	int TEXT = 23;
	int VERSION_DECL = 24;
	int NAMESPACE_DECL = 25;
	int DEF_NAMESPACE_DECL = 26;
	int DEF_COLLATION_DECL = 27;
	int DEF_FUNCTION_NS_DECL = 28;
	int ANNOT_DECL = 29;
	int GLOBAL_VAR = 30;
	int FUNCTION_DECL = 31;
	int FUNCTION_INLINE = 32;
	int FUNCTION_TEST = 33;
	int PROLOG = 34;
	int OPTION = 35;
	int ATOMIC_TYPE = 36;
	int MODULE = 37;
	int ORDER_BY = 38;
	int GROUP_BY = 39;
	int POSITIONAL_VAR = 40;
	int CATCH_ERROR_CODE = 41;
	int CATCH_ERROR_DESC = 42;
	int CATCH_ERROR_VAL = 43;
	int MODULE_DECL = 44;
	int MODULE_IMPORT = 45;
	int SCHEMA_IMPORT = 46;
	int ATTRIBUTE_TEST = 47;
	int COMP_ELEM_CONSTRUCTOR = 48;
	int COMP_ATTR_CONSTRUCTOR = 49;
	int COMP_TEXT_CONSTRUCTOR = 50;
	int COMP_COMMENT_CONSTRUCTOR = 51;
	int COMP_PI_CONSTRUCTOR = 52;
	int COMP_NS_CONSTRUCTOR = 53;
	int COMP_DOC_CONSTRUCTOR = 54;
	int PRAGMA = 55;
	int GTEQ = 56;
	int SEQUENCE = 57;
	int LITERAL_xpointer = 58;
	int LPAREN = 59;
	int RPAREN = 60;
	int NCNAME = 61;
	int LITERAL_xquery = 62;
	int LITERAL_version = 63;
	int SEMICOLON = 64;
	int LITERAL_module = 65;
	int LITERAL_namespace = 66;
	int EQ = 67;
	int STRING_LITERAL = 68;
	int LITERAL_declare = 69;
	int LITERAL_default = 70;
	// "boundary-space" = 71
	int LITERAL_ordering = 72;
	int LITERAL_construction = 73;
	// "base-uri" = 74
	// "copy-namespaces" = 75
	int LITERAL_option = 76;
	int LITERAL_function = 77;
	int LITERAL_variable = 78;
	int MOD = 79;
	int LITERAL_import = 80;
	int LITERAL_encoding = 81;
	int LITERAL_collation = 82;
	int LITERAL_element = 83;
	int LITERAL_order = 84;
	int LITERAL_empty = 85;
	int LITERAL_greatest = 86;
	int LITERAL_least = 87;
	int LITERAL_preserve = 88;
	int LITERAL_strip = 89;
	int LITERAL_ordered = 90;
	int LITERAL_unordered = 91;
	int COMMA = 92;
	// "no-preserve" = 93
	int LITERAL_inherit = 94;
	// "no-inherit" = 95
	int DOLLAR = 96;
	int LCURLY = 97;
	int RCURLY = 98;
	int COLON = 99;
	int LITERAL_external = 100;
	int LITERAL_schema = 101;
	// ":" = 102
	int LITERAL_as = 103;
	int LITERAL_at = 104;
	// "empty-sequence" = 105
	int QUESTION = 106;
	int STAR = 107;
	int PLUS = 108;
	int LITERAL_item = 109;
	int LITERAL_for = 110;
	int LITERAL_let = 111;
	int LITERAL_try = 112;
	int LITERAL_some = 113;
	int LITERAL_every = 114;
	int LITERAL_if = 115;
	int LITERAL_switch = 116;
	int LITERAL_typeswitch = 117;
	int LITERAL_update = 118;
	int LITERAL_replace = 119;
	int LITERAL_value = 120;
	int LITERAL_insert = 121;
	int LITERAL_delete = 122;
	int LITERAL_rename = 123;
	int LITERAL_with = 124;
	int LITERAL_into = 125;
	int LITERAL_preceding = 126;
	int LITERAL_following = 127;
	int LITERAL_catch = 128;
	int UNION = 129;
	int LITERAL_where = 130;
	int LITERAL_return = 131;
	int LITERAL_in = 132;
	int LITERAL_by = 133;
	int LITERAL_stable = 134;
	int LITERAL_ascending = 135;
	int LITERAL_descending = 136;
	int LITERAL_group = 137;
	int LITERAL_satisfies = 138;
	int LITERAL_case = 139;
	int LITERAL_then = 140;
	int LITERAL_else = 141;
	int LITERAL_or = 142;
	int LITERAL_and = 143;
	int LITERAL_instance = 144;
	int LITERAL_of = 145;
	int LITERAL_treat = 146;
	int LITERAL_castable = 147;
	int LITERAL_cast = 148;
	int BEFORE = 149;
	int AFTER = 150;
	int LITERAL_eq = 151;
	int LITERAL_ne = 152;
	int LITERAL_lt = 153;
	int LITERAL_le = 154;
	int LITERAL_gt = 155;
	int LITERAL_ge = 156;
	int GT = 157;
	int NEQ = 158;
	int LT = 159;
	int LTEQ = 160;
	int LITERAL_is = 161;
	int LITERAL_isnot = 162;
	int ANDEQ = 163;
	int OREQ = 164;
	int CONCAT = 165;
	int LITERAL_to = 166;
	int MINUS = 167;
	int LITERAL_div = 168;
	int LITERAL_idiv = 169;
	int LITERAL_mod = 170;
	int PRAGMA_START = 171;
	int PRAGMA_END = 172;
	int LITERAL_union = 173;
	int LITERAL_intersect = 174;
	int LITERAL_except = 175;
	int SLASH = 176;
	int DSLASH = 177;
	int LITERAL_text = 178;
	int LITERAL_node = 179;
	int LITERAL_attribute = 180;
	int LITERAL_comment = 181;
	// "processing-instruction" = 182
	// "document-node" = 183
	int LITERAL_document = 184;
	int HASH = 185;
	int SELF = 186;
	int XML_COMMENT = 187;
	int XML_PI = 188;
	int LPPAREN = 189;
	int RPPAREN = 190;
	int AT = 191;
	int PARENT = 192;
	int LITERAL_child = 193;
	int LITERAL_self = 194;
	int LITERAL_descendant = 195;
	// "descendant-or-self" = 196
	// "following-sibling" = 197
	int LITERAL_parent = 198;
	int LITERAL_ancestor = 199;
	// "ancestor-or-self" = 200
	// "preceding-sibling" = 201
	int DOUBLE_LITERAL = 202;
	int DECIMAL_LITERAL = 203;
	int INTEGER_LITERAL = 204;
	// "schema-element" = 205
	int END_TAG_START = 206;
	int QUOT = 207;
	int APOS = 208;
	int QUOT_ATTRIBUTE_CONTENT = 209;
	int ESCAPE_QUOT = 210;
	int APOS_ATTRIBUTE_CONTENT = 211;
	int ESCAPE_APOS = 212;
	int ELEMENT_CONTENT = 213;
	int XML_COMMENT_END = 214;
	int XML_PI_END = 215;
	int XML_CDATA = 216;
	int LITERAL_collection = 217;
	int LITERAL_validate = 218;
	int XML_PI_START = 219;
	int XML_CDATA_START = 220;
	int XML_CDATA_END = 221;
	int LETTER = 222;
	int DIGITS = 223;
	int HEX_DIGITS = 224;
	int NMSTART = 225;
	int NMCHAR = 226;
	int WS = 227;
	int EXPR_COMMENT = 228;
	int PREDEFINED_ENTITY_REF = 229;
	int CHAR_REF = 230;
	int S = 231;
	int NEXT_TOKEN = 232;
	int CHAR = 233;
	int BASECHAR = 234;
	int IDEOGRAPHIC = 235;
	int COMBINING_CHAR = 236;
	int DIGIT = 237;
	int EXTENDER = 238;
}
