use std::fmt::Display;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum TokenType {
    //Single char tokens
    OPENP,
    CLOSEP,
    PLUS,
    MINUS,
    NEWLINE,
    EOF,
    COMMA,
    DOT,
    SEMICOLON,
    ASTERISK,
    SLASH,
    LESS,
    GREATER,
    ASSIGN,
    BANG,
    //Multi char tokens
    IDENTIFIER,
    NUMBER,
    LESSEQ,
    GREATEREQ,
    NOTEQUALS,
    EQUALS,

    //Keywords
    //SQL has so many fucking keywords that I'm sad I implemented them this way
    CREATE,
    PRIMARY,
    ASC,
    DESC,
    AUTOINCREMENT,
    UNIQUE,
    KEY,
    SELECT,
    FROM,
    WHERE,
    TABLE,
    INTEGER,
    TEXT,
    OR,
    AND,
    NOT,
    NULL,
    TRUE,
    FALSE,
    STRING,
    ON,
    CONFLICT,
    ROLLBACK,
    ABORT,
    FAIL,
    IGNORE,
    REPLACE,
}

impl Display for TokenType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self {
            TokenType::OPENP => "OPENP",
            TokenType::CLOSEP => "CLOSEP",
            TokenType::PLUS => "PLUS",
            TokenType::MINUS => "MINUS",
            TokenType::NEWLINE => "NEWLINE",
            TokenType::EOF => "EOF",
            TokenType::COMMA => "COMMA",
            TokenType::DOT => "DOT",
            TokenType::SEMICOLON => "SEMICOLON",
            TokenType::IDENTIFIER => "IDENTIFIER",
            TokenType::NUMBER => "NUMBER",
            TokenType::CREATE => "CREATE",
            TokenType::PRIMARY => "PRIMARY",
            TokenType::KEY => "KEY",
            TokenType::SELECT => "SELECT",
            TokenType::FROM => "FROM",
            TokenType::WHERE => "WHERE",
            TokenType::TABLE => "TABLE",
            TokenType::INTEGER => "INTEGER",
            TokenType::TEXT => "TEXT",
            TokenType::ASTERISK => "ASTERISK",
            TokenType::OR => "OR",
            TokenType::AND => "AND",
            TokenType::NOTEQUALS => "NOT_EQUALS",
            TokenType::EQUALS => "EQUALS",
            TokenType::LESS => "LESS",
            TokenType::GREATER => "GREATER",
            TokenType::LESSEQ => "LESSEQ",
            TokenType::GREATEREQ => "GREATEREQ",
            TokenType::ASSIGN => "ASSIGN",
            TokenType::BANG => "BANG",
            TokenType::NOT => "NOT",
            TokenType::SLASH => "SLASH",
            TokenType::NULL => "NULL",
            TokenType::TRUE => "TRUE",
            TokenType::FALSE => "FALSE",
            TokenType::STRING => "STRING",
            TokenType::ASC => "ASC",
            TokenType::DESC => "DESC",
            TokenType::AUTOINCREMENT => "AUTOINCREMENT",
            TokenType::UNIQUE => "UNIQUE",
            TokenType::ON => "ON",
            TokenType::CONFLICT => "CONFLICT",
            TokenType::ROLLBACK => "ROLLBACK",
            TokenType::ABORT => "ABORT",
            TokenType::FAIL => "FAIL",
            TokenType::IGNORE => "IGNORE",
            TokenType::REPLACE => "REPLACE",
        };
        write!(f, "{val}")
    }
}
fn map_token(s: &str) -> Option<TokenType> {
    let t = match s.to_uppercase().as_str() {
        "CREATE" => TokenType::CREATE,
        "SELECT" => TokenType::SELECT,
        "TABLE" => TokenType::TABLE,
        "FROM" => TokenType::FROM,
        "WHERE" => TokenType::WHERE,
        "PRIMARY" => TokenType::PRIMARY,
        "KEY" => TokenType::KEY,
        "INTEGER" => TokenType::INTEGER,
        "TEXT" => TokenType::TEXT,
        "OR" => TokenType::OR,
        "AND" => TokenType::AND,
        "NOT" => TokenType::NOT,
        "NULL" => TokenType::NULL,
        "TRUE" => TokenType::TRUE,
        "FALSE" => TokenType::FALSE,
        "ASC" => TokenType::ASC,
        "DESC" => TokenType::DESC,
        "AUTOINCREMENT" => TokenType::AUTOINCREMENT,
        "UNIQUE" => TokenType::UNIQUE,
        "ON" => TokenType::ON,
        "CONFLICT" => TokenType::CONFLICT,
        "ROLLBACK" => TokenType::ROLLBACK,
        "ABORT" => TokenType::ABORT,
        "FAIL" => TokenType::FAIL,
        "IGNORE" => TokenType::IGNORE,
        "REPLACE" => TokenType::REPLACE,

        _ => return None,
    };
    Some(t)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Token<'a> {
    pub(super) typ: TokenType,
    pub(super) lexeme: &'a str,
    // TODO: sarebbe carino tenere un conto delle righe per i messaggi di errore
}

pub(crate) fn scan(s: &str) -> Vec<Token> {
    let mut toks = Vec::new();
    let mut it = s.as_bytes().iter().peekable();
    let mut cur = 0;
    while let Some(c) = it.next() {
        let c = *c as char;
        let tok = match c {
            '*' => Token {
                typ: TokenType::ASTERISK,
                lexeme: "*",
            },
            '+' => Token {
                typ: TokenType::PLUS,
                lexeme: "+",
            },
            '-' => Token {
                typ: TokenType::MINUS,
                lexeme: "-",
            },
            '(' => Token {
                typ: TokenType::OPENP,
                lexeme: "(",
            },
            ')' => Token {
                typ: TokenType::CLOSEP,
                lexeme: ")",
            },
            ',' => Token {
                typ: TokenType::COMMA,
                lexeme: ",",
            },
            ';' => Token {
                typ: TokenType::SEMICOLON,
                lexeme: ";",
            },
            '.' => Token {
                typ: TokenType::DOT,
                lexeme: ".",
            },
            '=' => Token {
                typ: TokenType::EQUALS,
                lexeme: "=",
            },
            '!' => match it.peek().expect("incomplete '!'") {
                b'=' => {
                    it.next();
                    cur += 1;
                    Token {
                        typ: TokenType::NOTEQUALS,
                        lexeme: "!=",
                    }
                }
                _ => Token {
                    typ: TokenType::BANG,
                    lexeme: "!",
                },
            },
            '<' => match it.peek().expect("incomplete '='") {
                b'=' => {
                    cur += 1;
                    it.next();
                    Token {
                        typ: TokenType::LESSEQ,
                        lexeme: "<=",
                    }
                }
                _ => Token {
                    typ: TokenType::LESS,
                    lexeme: "<",
                },
            },
            '>' => match it.peek().expect("incomplete '='") {
                b'=' => {
                    cur += 1;
                    it.next();
                    Token {
                        typ: TokenType::GREATEREQ,
                        lexeme: ">=",
                    }
                }
                _ => Token {
                    typ: TokenType::GREATER,
                    lexeme: ">",
                },
            },
            '/' => Token {
                typ: TokenType::SLASH,
                lexeme: "/",
            },
            '\'' => {
                let start = cur + 1;
                while let Some(&c) = it.peek() {
                    let c = *c as char;
                    if c != '\'' {
                        it.next();
                        cur += 1;
                    } else {
                        it.next();
                        cur += 1;
                        break;
                    }
                }
                let lexeme = &s[start..cur];
                Token {
                    typ: TokenType::STRING,
                    lexeme,
                }
            }
            '"' => {
                let start = cur + 1;
                while let Some(&c) = it.peek() {
                    let c = *c as char;
                    if c != '"' {
                        it.next();
                        cur += 1;
                    } else {
                        it.next();
                        cur += 1;
                        break;
                    }
                }
                let lexeme = &s[start..cur];
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme,
                }
            }
            'a'..='z' | 'A'..='Z' => {
                let start = cur;
                while let Some(&c) = it.peek() {
                    let c = *c as char;
                    if c.is_ascii_alphanumeric() || c == '_' {
                        it.next();
                        cur += 1;
                    } else {
                        break;
                    }
                }
                let lexeme = &s[(start)..=(cur)];
                if let Some(typ) = map_token(lexeme) {
                    Token { typ, lexeme }
                } else {
                    Token {
                        typ: TokenType::IDENTIFIER,
                        lexeme,
                    }
                }
            }
            '0'..='9' => {
                let start = cur;
                while let Some(&c) = it.peek() {
                    //println!("reading num {c}");
                    if !c.is_ascii_digit() && c != &b'.' {
                        break;
                    } else {
                        it.next();
                        cur += 1;
                    }
                }
                let lexeme = &s[(start)..=(cur)];
                Token {
                    typ: TokenType::NUMBER,
                    lexeme,
                }
            }
            ' ' | '\n' | '\t' | '\r' => {
                cur += 1;
                continue;
            }
            v => panic!("Unexpected character {v} in SQL"),
        };
        toks.push(tok);
        cur += 1;
    }
    toks.push(Token {
        typ: TokenType::EOF,
        lexeme: "EOF",
    });
    toks
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tokenize() {
        assert_eq!(
            vec![
                Token {
                    typ: TokenType::SELECT,
                    lexeme: "SELECT"
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "miao"
                },
                Token {
                    typ: TokenType::FROM,
                    lexeme: "FROM"
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "gatto"
                },
                Token {
                    typ: TokenType::EOF,
                    lexeme: "EOF"
                },
            ],
            scan("SELECT miao FROM gatto")
        );
        assert_eq!(
            vec![
                Token {
                    typ: TokenType::CREATE,
                    lexeme: "CREATE"
                },
                Token {
                    typ: TokenType::TABLE,
                    lexeme: "TABLE"
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "gatto"
                },
                Token {
                    typ: TokenType::OPENP,
                    lexeme: "("
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "miao"
                },
                Token {
                    typ: TokenType::TEXT,
                    lexeme: "TEXT"
                },
                Token {
                    typ: TokenType::COMMA,
                    lexeme: ","
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "id"
                },
                Token {
                    typ: TokenType::INTEGER,
                    lexeme: "INTEGER"
                },
                Token {
                    typ: TokenType::PRIMARY,
                    lexeme: "PRIMARY"
                },
                Token {
                    typ: TokenType::KEY,
                    lexeme: "KEY"
                },
                Token {
                    typ: TokenType::CLOSEP,
                    lexeme: ")"
                },
                Token {
                    typ: TokenType::SEMICOLON,
                    lexeme: ";"
                },
                Token {
                    typ: TokenType::EOF,
                    lexeme: "EOF"
                },
            ],
            scan("CREATE TABLE gatto(\n miao TEXT,\n id INTEGER PRIMARY KEY \n);")
        );
    }
    #[test]
    fn test_numbers() {
        let nums = "12 * 13 * (18 - 10 )/ 7 + 501 != 10";
        let nums = scan(nums);
        println!("{nums:?}");
    }
    #[test]
    fn test_schema() {
        let schema = "CREATE TABLE sqlite_schema(
  type text,
  name text,
  tbl_name text,
  rootpage integer,
  sql text
);";
        let schema = scan(schema);
        println!("{schema:?}");
    }
    #[test]
    fn test_where() {
        let query = "WHERE 10 = 10 AND 9 != 10 AND 'CIAO' != 'ciao'";
        let schema = scan(query);
        println!("{schema:?}");
    }
}
