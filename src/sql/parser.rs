//! A hand-written SQL lexer and recursive-descent parser for the dialect
//! quorumdb speaks: a practical subset of PostgreSQL.

use crate::sql::types::{DataType, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Literal(Value),
    /// `table.column` or `column`.
    Column(Option<String>, String),
    Unary(UnaryOp, Box<Expr>),
    Binary(Box<Expr>, BinOp, Box<Expr>),
    IsNull(Box<Expr>, bool),
    /// An aggregate: `COUNT(*)` is `Aggregate(Count, None)`.
    Aggregate(AggFn, Option<Box<Expr>>),
    Like(Box<Expr>, Box<Expr>, bool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: DataType,
    pub primary_key: bool,
    pub not_null: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Expr(Expr, Option<String>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    pub items: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<(Expr, bool)>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Select(Box<Select>),
    Update {
        table: String,
        set: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    Begin,
    Commit,
    Rollback,
    Explain(Box<Statement>),
    /// `SET x = y` and `SHOW x`: accepted for client compatibility.
    Set,
    ShowTables,
}

// ─── Lexer ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Ident(String),
    Keyword(String),
    Number(String),
    Str(String),
    Sym(&'static str),
}

const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "TABLE",
    "DROP",
    "IF",
    "NOT",
    "EXISTS",
    "PRIMARY",
    "KEY",
    "NULL",
    "AND",
    "OR",
    "IS",
    "TRUE",
    "FALSE",
    "ORDER",
    "BY",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "GROUP",
    "HAVING",
    "AS",
    "JOIN",
    "INNER",
    "LEFT",
    "OUTER",
    "ON",
    "BEGIN",
    "START",
    "TRANSACTION",
    "COMMIT",
    "ROLLBACK",
    "END",
    "EXPLAIN",
    "COUNT",
    "SUM",
    "MIN",
    "MAX",
    "AVG",
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TEXT",
    "VARCHAR",
    "CHAR",
    "BOOLEAN",
    "BOOL",
    "FLOAT",
    "DOUBLE",
    "PRECISION",
    "REAL",
    "NUMERIC",
    "DECIMAL",
    "SHOW",
    "TABLES",
    "LIKE",
    "SERIAL",
    "WORK",
    "ABORT",
];

fn lex(sql: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let upper = word.to_ascii_uppercase();
            if KEYWORDS.contains(&upper.as_str()) {
                out.push(Token::Keyword(upper));
            } else {
                out.push(Token::Ident(word.to_ascii_lowercase()));
            }
        } else if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()))
        {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            out.push(Token::Number(chars[start..i].iter().collect()));
        } else if c == '\'' {
            let mut s = String::new();
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err("unterminated string literal".into()),
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                        s.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(&ch) => {
                        s.push(ch);
                        i += 1;
                    }
                }
            }
            out.push(Token::Str(s));
        } else if c == '"' {
            let start = i + 1;
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                i += 1;
            }
            if i >= chars.len() {
                return Err("unterminated quoted identifier".into());
            }
            out.push(Token::Ident(chars[start..i].iter().collect()));
            i += 1;
        } else {
            let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
            let sym = match two.as_str() {
                "<=" => Some("<="),
                ">=" => Some(">="),
                "<>" | "!=" => Some("<>"),
                "||" => Some("||"),
                _ => None,
            };
            if let Some(s) = sym {
                out.push(Token::Sym(s));
                i += 2;
                continue;
            }
            let sym = match c {
                '(' => "(",
                ')' => ")",
                ',' => ",",
                ';' => ";",
                '*' => "*",
                '+' => "+",
                '-' => "-",
                '/' => "/",
                '%' => "%",
                '=' => "=",
                '<' => "<",
                '>' => ">",
                '.' => ".",
                _ => return Err(format!("unexpected character {c:?}")),
            };
            out.push(Token::Sym(sym));
            i += 1;
        }
    }
    Ok(out)
}

// ─── Parser ───────────────────────────────────────────────────────────────

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

type PResult<T> = Result<T, String>;

/// Parse a string of one or more `;`-separated statements.
pub fn parse(sql: &str) -> PResult<Vec<Statement>> {
    let mut p = Parser {
        tokens: lex(sql)?,
        pos: 0,
    };
    let mut stmts = Vec::new();
    loop {
        while p.eat_sym(";") {}
        if p.pos >= p.tokens.len() {
            return Ok(stmts);
        }
        stmts.push(p.statement()?);
        if p.pos < p.tokens.len() && !p.eat_sym(";") {
            return Err(format!("syntax error at {}", p.here()));
        }
    }
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn here(&self) -> String {
        match self.peek() {
            None => "end of input".to_string(),
            Some(Token::Ident(s) | Token::Keyword(s) | Token::Number(s)) => format!("{s:?}"),
            Some(Token::Str(s)) => format!("'{s}'"),
            Some(Token::Sym(s)) => format!("{s:?}"),
        }
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if matches!(self.peek(), Some(Token::Keyword(k)) if k == kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> PResult<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(format!("syntax error: expected {kw} at {}", self.here()))
        }
    }

    fn eat_sym(&mut self, s: &str) -> bool {
        if matches!(self.peek(), Some(Token::Sym(t)) if *t == s) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_sym(&mut self, s: &str) -> PResult<()> {
        if self.eat_sym(s) {
            Ok(())
        } else {
            Err(format!("syntax error: expected {s:?} at {}", self.here()))
        }
    }

    fn ident(&mut self) -> PResult<String> {
        match self.peek().cloned() {
            Some(Token::Ident(s)) => {
                self.pos += 1;
                Ok(s)
            }
            // Many keywords double as names (e.g. a column called "key").
            Some(Token::Keyword(k))
                if !matches!(
                    k.as_str(),
                    "SELECT"
                        | "FROM"
                        | "WHERE"
                        | "AND"
                        | "OR"
                        | "NOT"
                        | "ON"
                        | "JOIN"
                        | "ORDER"
                        | "GROUP"
                        | "LIMIT"
                        | "SET"
                        | "VALUES"
                        | "LEFT"
                        | "INNER"
                        | "HAVING"
                        | "OFFSET"
                ) =>
            {
                self.pos += 1;
                Ok(k.to_ascii_lowercase())
            }
            _ => Err(format!("syntax error: expected a name at {}", self.here())),
        }
    }

    fn statement(&mut self) -> PResult<Statement> {
        if self.eat_kw("EXPLAIN") {
            return Ok(Statement::Explain(Box::new(self.statement()?)));
        }
        if self.eat_kw("SELECT") {
            return Ok(Statement::Select(Box::new(self.select()?)));
        }
        if self.eat_kw("INSERT") {
            return self.insert();
        }
        if self.eat_kw("UPDATE") {
            return self.update();
        }
        if self.eat_kw("DELETE") {
            self.expect_kw("FROM")?;
            let table = self.ident()?;
            let filter = if self.eat_kw("WHERE") {
                Some(self.expr()?)
            } else {
                None
            };
            return Ok(Statement::Delete { table, filter });
        }
        if self.eat_kw("CREATE") {
            self.expect_kw("TABLE")?;
            return self.create_table();
        }
        if self.eat_kw("DROP") {
            self.expect_kw("TABLE")?;
            let if_exists = self.eat_kw("IF") && {
                self.expect_kw("EXISTS")?;
                true
            };
            return Ok(Statement::DropTable {
                name: self.ident()?,
                if_exists,
            });
        }
        if self.eat_kw("BEGIN") || (self.eat_kw("START") && self.eat_kw("TRANSACTION")) {
            self.eat_kw("TRANSACTION");
            self.eat_kw("WORK");
            return Ok(Statement::Begin);
        }
        if self.eat_kw("COMMIT") || self.eat_kw("END") {
            self.eat_kw("TRANSACTION");
            self.eat_kw("WORK");
            return Ok(Statement::Commit);
        }
        if self.eat_kw("ROLLBACK") || self.eat_kw("ABORT") {
            self.eat_kw("TRANSACTION");
            self.eat_kw("WORK");
            return Ok(Statement::Rollback);
        }
        if self.eat_kw("SHOW") {
            if self.eat_kw("TABLES") {
                return Ok(Statement::ShowTables);
            }
            while self.pos < self.tokens.len() && !matches!(self.peek(), Some(Token::Sym(";"))) {
                self.pos += 1;
            }
            return Ok(Statement::Set);
        }
        if self.eat_kw("SET") {
            while self.pos < self.tokens.len() && !matches!(self.peek(), Some(Token::Sym(";"))) {
                self.pos += 1;
            }
            return Ok(Statement::Set);
        }
        Err(format!("syntax error at {}", self.here()))
    }

    fn data_type(&mut self) -> PResult<(DataType, bool)> {
        let Some(Token::Keyword(k)) = self.peek().cloned() else {
            return Err(format!("syntax error: expected a type at {}", self.here()));
        };
        self.pos += 1;
        let ty = match k.as_str() {
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => DataType::Int,
            "SERIAL" => return Ok((DataType::Int, true)),
            "TEXT" | "VARCHAR" | "CHAR" => DataType::Text,
            "BOOLEAN" | "BOOL" => DataType::Bool,
            "FLOAT" | "REAL" | "NUMERIC" | "DECIMAL" => DataType::Float,
            "DOUBLE" => {
                self.eat_kw("PRECISION");
                DataType::Float
            }
            other => return Err(format!("unknown type {other}")),
        };
        // VARCHAR(20), NUMERIC(10, 2): sizes are accepted and ignored.
        if self.eat_sym("(") {
            while !self.eat_sym(")") {
                if self.pos >= self.tokens.len() {
                    return Err("unterminated type modifier".into());
                }
                self.pos += 1;
            }
        }
        Ok((ty, false))
    }

    fn create_table(&mut self) -> PResult<Statement> {
        let if_not_exists = self.eat_kw("IF") && {
            self.expect_kw("NOT")?;
            self.expect_kw("EXISTS")?;
            true
        };
        let name = self.ident()?;
        self.expect_sym("(")?;
        let mut columns: Vec<ColumnDef> = Vec::new();
        loop {
            if self.eat_kw("PRIMARY") {
                self.expect_kw("KEY")?;
                self.expect_sym("(")?;
                let pk = self.ident()?;
                self.expect_sym(")")?;
                let col = columns
                    .iter_mut()
                    .find(|c| c.name == pk)
                    .ok_or_else(|| format!("primary key column {pk} is not defined"))?;
                col.primary_key = true;
                col.not_null = true;
            } else {
                let cname = self.ident()?;
                let (ty, serial) = self.data_type()?;
                let mut col = ColumnDef {
                    name: cname,
                    ty,
                    primary_key: serial,
                    not_null: serial,
                };
                loop {
                    if self.eat_kw("PRIMARY") {
                        self.expect_kw("KEY")?;
                        col.primary_key = true;
                        col.not_null = true;
                    } else if self.eat_kw("NOT") {
                        self.expect_kw("NULL")?;
                        col.not_null = true;
                    } else if self.eat_kw("NULL") {
                    } else {
                        break;
                    }
                }
                columns.push(col);
            }
            if !self.eat_sym(",") {
                break;
            }
        }
        self.expect_sym(")")?;
        if columns.iter().filter(|c| c.primary_key).count() > 1 {
            return Err("only a single-column primary key is supported".into());
        }
        Ok(Statement::CreateTable {
            name,
            columns,
            if_not_exists,
        })
    }

    fn insert(&mut self) -> PResult<Statement> {
        self.expect_kw("INTO")?;
        let table = self.ident()?;
        let columns = if self.eat_sym("(") {
            let mut cols = vec![self.ident()?];
            while self.eat_sym(",") {
                cols.push(self.ident()?);
            }
            self.expect_sym(")")?;
            Some(cols)
        } else {
            None
        };
        self.expect_kw("VALUES")?;
        let mut rows = Vec::new();
        loop {
            self.expect_sym("(")?;
            let mut row = vec![self.expr()?];
            while self.eat_sym(",") {
                row.push(self.expr()?);
            }
            self.expect_sym(")")?;
            rows.push(row);
            if !self.eat_sym(",") {
                break;
            }
        }
        Ok(Statement::Insert {
            table,
            columns,
            rows,
        })
    }

    fn update(&mut self) -> PResult<Statement> {
        let table = self.ident()?;
        self.expect_kw("SET")?;
        let mut set = Vec::new();
        loop {
            let col = self.ident()?;
            self.expect_sym("=")?;
            set.push((col, self.expr()?));
            if !self.eat_sym(",") {
                break;
            }
        }
        let filter = if self.eat_kw("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Statement::Update { table, set, filter })
    }

    fn table_ref(&mut self) -> PResult<TableRef> {
        let name = self.ident()?;
        let alias = if self.eat_kw("AS") || matches!(self.peek(), Some(Token::Ident(_))) {
            Some(self.ident()?)
        } else {
            None
        };
        Ok(TableRef { name, alias })
    }

    fn select(&mut self) -> PResult<Select> {
        let mut items = Vec::new();
        loop {
            if self.eat_sym("*") {
                items.push(SelectItem::Wildcard);
            } else {
                let e = self.expr()?;
                let alias = if self.eat_kw("AS") || matches!(self.peek(), Some(Token::Ident(_))) {
                    Some(self.ident()?)
                } else {
                    None
                };
                items.push(SelectItem::Expr(e, alias));
            }
            if !self.eat_sym(",") {
                break;
            }
        }
        let mut sel = Select {
            items,
            from: None,
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        if self.eat_kw("FROM") {
            sel.from = Some(self.table_ref()?);
            loop {
                let kind = if self.eat_kw("JOIN") {
                    JoinKind::Inner
                } else if self.eat_kw("INNER") {
                    self.expect_kw("JOIN")?;
                    JoinKind::Inner
                } else if self.eat_kw("LEFT") {
                    self.eat_kw("OUTER");
                    self.expect_kw("JOIN")?;
                    JoinKind::Left
                } else {
                    break;
                };
                let table = self.table_ref()?;
                self.expect_kw("ON")?;
                let on = self.expr()?;
                sel.joins.push(Join { kind, table, on });
            }
        }
        if self.eat_kw("WHERE") {
            sel.filter = Some(self.expr()?);
        }
        if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            loop {
                sel.group_by.push(self.expr()?);
                if !self.eat_sym(",") {
                    break;
                }
            }
        }
        if self.eat_kw("HAVING") {
            sel.having = Some(self.expr()?);
        }
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            loop {
                let e = self.expr()?;
                let desc = if self.eat_kw("DESC") {
                    true
                } else {
                    self.eat_kw("ASC");
                    false
                };
                sel.order_by.push((e, desc));
                if !self.eat_sym(",") {
                    break;
                }
            }
        }
        if self.eat_kw("LIMIT") {
            sel.limit = Some(self.unsigned()?);
        }
        if self.eat_kw("OFFSET") {
            sel.offset = Some(self.unsigned()?);
        }
        Ok(sel)
    }

    fn unsigned(&mut self) -> PResult<u64> {
        match self.peek().cloned() {
            Some(Token::Number(n)) => {
                self.pos += 1;
                n.parse()
                    .map_err(|_| format!("expected a whole number, got {n}"))
            }
            _ => Err(format!(
                "syntax error: expected a number at {}",
                self.here()
            )),
        }
    }

    // Precedence, lowest first: OR, AND, NOT, comparison, ||, + -, * / %, unary.
    fn expr(&mut self) -> PResult<Expr> {
        let mut left = self.and_expr()?;
        while self.eat_kw("OR") {
            left = Expr::Binary(Box::new(left), BinOp::Or, Box::new(self.and_expr()?));
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> PResult<Expr> {
        let mut left = self.not_expr()?;
        while self.eat_kw("AND") {
            left = Expr::Binary(Box::new(left), BinOp::And, Box::new(self.not_expr()?));
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> PResult<Expr> {
        if self.eat_kw("NOT") {
            return Ok(Expr::Unary(UnaryOp::Not, Box::new(self.not_expr()?)));
        }
        self.comparison()
    }

    fn comparison(&mut self) -> PResult<Expr> {
        let left = self.concat()?;
        if self.eat_kw("IS") {
            let not = self.eat_kw("NOT");
            self.expect_kw("NULL")?;
            return Ok(Expr::IsNull(Box::new(left), not));
        }
        let negated = self.eat_kw("NOT");
        if self.eat_kw("LIKE") {
            return Ok(Expr::Like(
                Box::new(left),
                Box::new(self.concat()?),
                negated,
            ));
        }
        if negated {
            return Err(format!("syntax error: expected LIKE at {}", self.here()));
        }
        let op = match self.peek() {
            Some(Token::Sym("=")) => BinOp::Eq,
            Some(Token::Sym("<>")) => BinOp::NotEq,
            Some(Token::Sym("<")) => BinOp::Lt,
            Some(Token::Sym("<=")) => BinOp::LtEq,
            Some(Token::Sym(">")) => BinOp::Gt,
            Some(Token::Sym(">=")) => BinOp::GtEq,
            _ => return Ok(left),
        };
        self.pos += 1;
        Ok(Expr::Binary(Box::new(left), op, Box::new(self.concat()?)))
    }

    fn concat(&mut self) -> PResult<Expr> {
        let mut left = self.additive()?;
        while self.eat_sym("||") {
            left = Expr::Binary(Box::new(left), BinOp::Concat, Box::new(self.additive()?));
        }
        Ok(left)
    }

    fn additive(&mut self) -> PResult<Expr> {
        let mut left = self.multiplicative()?;
        loop {
            let op = if self.eat_sym("+") {
                BinOp::Add
            } else if self.eat_sym("-") {
                BinOp::Sub
            } else {
                return Ok(left);
            };
            left = Expr::Binary(Box::new(left), op, Box::new(self.multiplicative()?));
        }
    }

    fn multiplicative(&mut self) -> PResult<Expr> {
        let mut left = self.unary()?;
        loop {
            let op = if self.eat_sym("*") {
                BinOp::Mul
            } else if self.eat_sym("/") {
                BinOp::Div
            } else if self.eat_sym("%") {
                BinOp::Mod
            } else {
                return Ok(left);
            };
            left = Expr::Binary(Box::new(left), op, Box::new(self.unary()?));
        }
    }

    fn unary(&mut self) -> PResult<Expr> {
        if self.eat_sym("-") {
            return Ok(Expr::Unary(UnaryOp::Neg, Box::new(self.unary()?)));
        }
        if self.eat_sym("+") {
            return self.unary();
        }
        self.primary()
    }

    fn primary(&mut self) -> PResult<Expr> {
        let tok = self.peek().cloned();
        match tok {
            Some(Token::Number(n)) => {
                self.pos += 1;
                if n.contains('.') {
                    n.parse()
                        .map(|f| Expr::Literal(Value::Float(f)))
                        .map_err(|_| format!("bad number {n}"))
                } else {
                    n.parse()
                        .map(|i| Expr::Literal(Value::Int(i)))
                        .map_err(|_| format!("bad number {n}"))
                }
            }
            Some(Token::Str(s)) => {
                self.pos += 1;
                Ok(Expr::Literal(Value::Text(s)))
            }
            Some(Token::Sym("(")) => {
                self.pos += 1;
                let e = self.expr()?;
                self.expect_sym(")")?;
                Ok(e)
            }
            Some(Token::Keyword(k)) if k == "NULL" => {
                self.pos += 1;
                Ok(Expr::Literal(Value::Null))
            }
            Some(Token::Keyword(k)) if k == "TRUE" || k == "FALSE" => {
                self.pos += 1;
                Ok(Expr::Literal(Value::Bool(k == "TRUE")))
            }
            Some(Token::Keyword(k))
                if matches!(k.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG") =>
            {
                self.pos += 1;
                let f = match k.as_str() {
                    "COUNT" => AggFn::Count,
                    "SUM" => AggFn::Sum,
                    "MIN" => AggFn::Min,
                    "MAX" => AggFn::Max,
                    _ => AggFn::Avg,
                };
                self.expect_sym("(")?;
                let arg = if f == AggFn::Count && self.eat_sym("*") {
                    None
                } else {
                    Some(Box::new(self.expr()?))
                };
                self.expect_sym(")")?;
                Ok(Expr::Aggregate(f, arg))
            }
            _ => {
                let name = self.ident()?;
                if self.eat_sym(".") {
                    let col = self.ident()?;
                    Ok(Expr::Column(Some(name), col))
                } else {
                    Ok(Expr::Column(None, name))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_query() {
        let stmts = parse(
            "SELECT c.name, COUNT(*) AS orders, SUM(o.total) FROM customers c \
             JOIN orders o ON o.customer_id = c.id WHERE o.total > 10 AND NOT c.banned \
             GROUP BY c.name HAVING COUNT(*) > 1 ORDER BY orders DESC, c.name LIMIT 5 OFFSET 2;",
        )
        .unwrap();
        let Statement::Select(s) = &stmts[0] else {
            panic!()
        };
        assert_eq!(s.items.len(), 3);
        assert_eq!(s.joins.len(), 1);
        assert_eq!(s.group_by.len(), 1);
        assert!(s.order_by[0].1);
        assert_eq!((s.limit, s.offset), (Some(5), Some(2)));
    }

    #[test]
    fn parses_ddl_and_dml() {
        let stmts = parse(
            "CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, owner VARCHAR(40) NOT NULL, balance BIGINT);
             INSERT INTO accounts VALUES (1, 'a''s', 100), (2, 'b', -5);
             UPDATE accounts SET balance = balance - 10 WHERE id = 1;
             DELETE FROM accounts WHERE balance < 0; BEGIN; COMMIT;",
        )
        .unwrap();
        assert_eq!(stmts.len(), 6);
        let Statement::Insert { rows, .. } = &stmts[1] else {
            panic!()
        };
        assert_eq!(rows[0][1], Expr::Literal(Value::Text("a's".into())));
        assert!(parse("SELECT FROM").is_err());
    }
}
