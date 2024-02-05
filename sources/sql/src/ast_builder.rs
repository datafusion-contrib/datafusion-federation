use core::fmt;

use datafusion::sql::sqlparser::ast;

#[derive(Clone)]
#[doc = "Builder for [`Query`](struct.Query.html).\n"]
pub struct QueryBuilder {
    with: Option<ast::With>,
    body: Option<Box<ast::SetExpr>>,
    order_by: Vec<ast::OrderByExpr>,
    limit: Option<ast::Expr>,
    limit_by: Vec<ast::Expr>,
    offset: Option<ast::Offset>,
    fetch: Option<ast::Fetch>,
    locks: Vec<ast::LockClause>,
    for_clause: Option<ast::ForClause>,
}

#[allow(dead_code)]
impl QueryBuilder {
    #[allow(unused_mut)]
    pub fn with(&mut self, value: Option<ast::With>) -> &mut Self {
        let mut new = self;
        new.with = value;
        new
    }
    #[allow(unused_mut)]
    pub fn body(&mut self, value: Box<ast::SetExpr>) -> &mut Self {
        let mut new = self;
        new.body = Option::Some(value);
        new
    }
    #[allow(unused_mut)]
    pub fn order_by(&mut self, value: Vec<ast::OrderByExpr>) -> &mut Self {
        let mut new = self;
        new.order_by = value;
        new
    }
    #[allow(unused_mut)]
    pub fn limit(&mut self, value: Option<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.limit = value;
        new
    }
    #[allow(unused_mut)]
    pub fn limit_by(&mut self, value: Vec<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.limit_by = value;
        new
    }
    #[allow(unused_mut)]
    pub fn offset(&mut self, value: Option<ast::Offset>) -> &mut Self {
        let mut new = self;
        new.offset = value;
        new
    }
    #[allow(unused_mut)]
    pub fn fetch(&mut self, value: Option<ast::Fetch>) -> &mut Self {
        let mut new = self;
        new.fetch = value;
        new
    }
    #[allow(unused_mut)]
    pub fn locks(&mut self, value: Vec<ast::LockClause>) -> &mut Self {
        let mut new = self;
        new.locks = value;
        new
    }
    #[allow(unused_mut)]
    pub fn for_clause(&mut self, value: Option<ast::ForClause>) -> &mut Self {
        let mut new = self;
        new.for_clause = value;
        new
    }
    #[doc = "Builds a new `Query`.\n\n# Errors\n\nIf a required field has not been initialized.\n"]
    pub fn build(&self) -> Result<ast::Query, BuilderError> {
        Ok(ast::Query {
            with: self.with.clone(),
            body: match self.body {
                Some(ref value) => value.clone(),
                None => return Result::Err(Into::into(UninitializedFieldError::from("body"))),
            },
            order_by: self.order_by.clone(),
            limit: self.limit.clone(),
            limit_by: self.limit_by.clone(),
            offset: self.offset.clone(),
            fetch: self.fetch.clone(),
            locks: self.locks.clone(),
            for_clause: self.for_clause.clone(),
        })
    }
    #[doc = r" Create an empty builder, with all fields set to `None` or `PhantomData`."]
    fn create_empty() -> Self {
        Self {
            with: Default::default(),
            body: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            limit_by: Default::default(),
            offset: Default::default(),
            fetch: Default::default(),
            locks: Default::default(),
            for_clause: Default::default(),
        }
    }
}
impl Default for QueryBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
#[doc = "Builder for [`Select`](struct.Select.html).\n"]
pub struct SelectBuilder {
    distinct: Option<ast::Distinct>,
    top: Option<ast::Top>,
    projection: Vec<ast::SelectItem>,
    into: Option<ast::SelectInto>,
    from: Vec<TableWithJoinsBuilder>,
    lateral_views: Vec<ast::LateralView>,
    selection: Option<ast::Expr>,
    group_by: Option<ast::GroupByExpr>,
    cluster_by: Vec<ast::Expr>,
    distribute_by: Vec<ast::Expr>,
    sort_by: Vec<ast::Expr>,
    having: Option<ast::Expr>,
    named_window: Vec<ast::NamedWindowDefinition>,
    qualify: Option<ast::Expr>,
}
#[allow(dead_code)]
impl SelectBuilder {
    #[allow(unused_mut)]
    pub fn distinct(&mut self, value: Option<ast::Distinct>) -> &mut Self {
        let mut new = self;
        new.distinct = value;
        new
    }
    #[allow(unused_mut)]
    pub fn top(&mut self, value: Option<ast::Top>) -> &mut Self {
        let mut new = self;
        new.top = value;
        new
    }
    #[allow(unused_mut)]
    pub fn projection(&mut self, value: Vec<ast::SelectItem>) -> &mut Self {
        let mut new = self;
        new.projection = value;
        new
    }
    #[allow(unused_mut)]
    pub fn into(&mut self, value: Option<ast::SelectInto>) -> &mut Self {
        let mut new = self;
        new.into = value;
        new
    }
    #[allow(unused_mut)]
    pub fn from(&mut self, value: Vec<TableWithJoinsBuilder>) -> &mut Self {
        let mut new = self;
        new.from = value;
        new
    }
    #[allow(unused_mut)]
    pub fn push_from(&mut self, value: TableWithJoinsBuilder) -> &mut Self {
        let mut new = self;
        new.from.push(value);
        new
    }
    #[allow(unused_mut)]
    pub fn pop_from(&mut self) -> Option<TableWithJoinsBuilder> {
        self.from.pop()
    }
    #[allow(unused_mut)]
    pub fn lateral_views(&mut self, value: Vec<ast::LateralView>) -> &mut Self {
        let mut new = self;
        new.lateral_views = value;
        new
    }
    #[allow(unused_mut)]
    pub fn selection(&mut self, value: Option<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.selection = value;
        new
    }
    #[allow(unused_mut)]
    pub fn group_by(&mut self, value: ast::GroupByExpr) -> &mut Self {
        let mut new = self;
        new.group_by = Option::Some(value);
        new
    }
    #[allow(unused_mut)]
    pub fn cluster_by(&mut self, value: Vec<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.cluster_by = value;
        new
    }
    #[allow(unused_mut)]
    pub fn distribute_by(&mut self, value: Vec<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.distribute_by = value;
        new
    }
    #[allow(unused_mut)]
    pub fn sort_by(&mut self, value: Vec<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.sort_by = value;
        new
    }
    #[allow(unused_mut)]
    pub fn having(&mut self, value: Option<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.having = value;
        new
    }
    #[allow(unused_mut)]
    pub fn named_window(&mut self, value: Vec<ast::NamedWindowDefinition>) -> &mut Self {
        let mut new = self;
        new.named_window = value;
        new
    }
    #[allow(unused_mut)]
    pub fn qualify(&mut self, value: Option<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.qualify = value;
        new
    }
    #[doc = "Builds a new `Select`.\n\n# Errors\n\nIf a required field has not been initialized.\n"]
    pub fn build(&self) -> Result<ast::Select, BuilderError> {
        Ok(ast::Select {
            distinct: self.distinct.clone(),
            top: self.top.clone(),
            projection: self.projection.clone(),
            into: self.into.clone(),
            from: self
                .from
                .iter()
                .map(|b| b.build())
                .collect::<Result<Vec<_>, BuilderError>>()?,
            lateral_views: self.lateral_views.clone(),
            selection: self.selection.clone(),
            group_by: match self.group_by {
                Some(ref value) => value.clone(),
                None => return Result::Err(Into::into(UninitializedFieldError::from("group_by"))),
            },
            cluster_by: self.cluster_by.clone(),
            distribute_by: self.distribute_by.clone(),
            sort_by: self.sort_by.clone(),
            having: self.having.clone(),
            named_window: self.named_window.clone(),
            qualify: self.qualify.clone(),
        })
    }
    #[doc = r" Create an empty builder, with all fields set to `None` or `PhantomData`."]
    fn create_empty() -> Self {
        Self {
            distinct: Default::default(),
            top: Default::default(),
            projection: Default::default(),
            into: Default::default(),
            from: Default::default(),
            lateral_views: Default::default(),
            selection: Default::default(),
            group_by: Some(ast::GroupByExpr::Expressions(Vec::new())),
            cluster_by: Default::default(),
            distribute_by: Default::default(),
            sort_by: Default::default(),
            having: Default::default(),
            named_window: Default::default(),
            qualify: Default::default(),
        }
    }
}
impl Default for SelectBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
#[doc = "Builder for [`TableWithJoins`](struct.TableWithJoins.html).\n"]
pub struct TableWithJoinsBuilder {
    relation: Option<RelationBuilder>,
    joins: Vec<ast::Join>,
}
#[allow(dead_code)]
impl TableWithJoinsBuilder {
    #[allow(unused_mut)]
    pub fn relation(&mut self, value: RelationBuilder) -> &mut Self {
        let mut new = self;
        new.relation = Option::Some(value);
        new
    }

    #[allow(unused_mut)]
    pub fn joins(&mut self, value: Vec<ast::Join>) -> &mut Self {
        let mut new = self;
        new.joins = value;
        new
    }
    #[allow(unused_mut)]
    pub fn push_join(&mut self, value: ast::Join) -> &mut Self {
        let mut new = self;
        new.joins.push(value);
        new
    }

    #[doc = "Builds a new `TableWithJoins`.\n\n# Errors\n\nIf a required field has not been initialized.\n"]
    pub fn build(&self) -> Result<ast::TableWithJoins, BuilderError> {
        Ok(ast::TableWithJoins {
            relation: match self.relation {
                Some(ref value) => value.build()?,
                None => return Result::Err(Into::into(UninitializedFieldError::from("relation"))),
            },
            joins: self.joins.clone(),
        })
    }
    #[doc = r" Create an empty builder, with all fields set to `None` or `PhantomData`."]
    fn create_empty() -> Self {
        Self {
            relation: Default::default(),
            joins: Default::default(),
        }
    }
}
impl Default for TableWithJoinsBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
pub struct RelationBuilder {
    relation: Option<TableFactorBuilder>,
}

#[derive(Clone)]
enum TableFactorBuilder {
    Table(TableRelationBuilder),
    Derived(DerivedRelationBuilder),
}

#[allow(dead_code)]
impl RelationBuilder {
    #[allow(unused_mut)]
    pub fn has_relation(&self) -> bool {
        self.relation.is_some()
    }
    #[allow(unused_mut)]
    pub fn table(&mut self, value: TableRelationBuilder) -> &mut Self {
        let mut new = self;
        new.relation = Option::Some(TableFactorBuilder::Table(value));
        new
    }
    #[allow(unused_mut)]
    pub fn derived(&mut self, value: DerivedRelationBuilder) -> &mut Self {
        let mut new = self;
        new.relation = Option::Some(TableFactorBuilder::Derived(value));
        new
    }
    #[allow(unused_mut)]
    pub fn alias(&mut self, value: Option<ast::TableAlias>) -> &mut Self {
        let mut new = self;
        match new.relation {
            Some(TableFactorBuilder::Table(ref mut rel_builder)) => {
                rel_builder.alias = value;
            }
            Some(TableFactorBuilder::Derived(ref mut rel_builder)) => {
                rel_builder.alias = value;
            }
            None => (),
        }
        new
    }
    pub fn build(&self) -> Result<ast::TableFactor, BuilderError> {
        Ok(match self.relation {
            Some(TableFactorBuilder::Table(ref value)) => value.build()?,
            Some(TableFactorBuilder::Derived(ref value)) => value.build()?,
            None => return Result::Err(Into::into(UninitializedFieldError::from("relation"))),
        })
    }
    #[doc = r" Create an empty builder, with all fields set to `None` or `PhantomData`."]
    fn create_empty() -> Self {
        Self {
            relation: Default::default(),
        }
    }
}
impl Default for RelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

#[derive(Clone)]
#[doc = "Builder for [`Table`](struct.Table.html).\n"]
pub struct TableRelationBuilder {
    name: Option<ast::ObjectName>,
    alias: Option<ast::TableAlias>,
    args: Option<Vec<ast::FunctionArg>>,
    with_hints: Vec<ast::Expr>,
    version: Option<ast::TableVersion>,
    partitions: Vec<ast::Ident>,
}
#[allow(dead_code)]
impl TableRelationBuilder {
    #[allow(unused_mut)]
    pub fn name(&mut self, value: ast::ObjectName) -> &mut Self {
        let mut new = self;
        new.name = Option::Some(value);
        new
    }
    #[allow(unused_mut)]
    pub fn alias(&mut self, value: Option<ast::TableAlias>) -> &mut Self {
        let mut new = self;
        new.alias = value;
        new
    }
    #[allow(unused_mut)]
    pub fn args(&mut self, value: Option<Vec<ast::FunctionArg>>) -> &mut Self {
        let mut new = self;
        new.args = value;
        new
    }
    #[allow(unused_mut)]
    pub fn with_hints(&mut self, value: Vec<ast::Expr>) -> &mut Self {
        let mut new = self;
        new.with_hints = value;
        new
    }
    #[allow(unused_mut)]
    pub fn version(&mut self, value: Option<ast::TableVersion>) -> &mut Self {
        let mut new = self;
        new.version = value;
        new
    }
    #[allow(unused_mut)]
    pub fn partitions(&mut self, value: Vec<ast::Ident>) -> &mut Self {
        let mut new = self;
        new.partitions = value;
        new
    }
    #[doc = "Builds a new `Table`.\n\n# Errors\n\nIf a required field has not been initialized.\n"]
    pub fn build(&self) -> Result<ast::TableFactor, BuilderError> {
        Ok(ast::TableFactor::Table {
            name: match self.name {
                Some(ref value) => value.clone(),
                None => return Result::Err(Into::into(UninitializedFieldError::from("name"))),
            },
            alias: self.alias.clone(),
            args: self.args.clone(),
            with_hints: self.with_hints.clone(),
            version: self.version.clone(),
            partitions: self.partitions.clone(),
        })
    }
    #[doc = r" Create an empty builder, with all fields set to `None` or `PhantomData`."]
    fn create_empty() -> Self {
        Self {
            name: Default::default(),
            alias: Default::default(),
            args: Default::default(),
            with_hints: Default::default(),
            version: Default::default(),
            partitions: Default::default(),
        }
    }
}
impl Default for TableRelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}
#[derive(Clone)]
#[doc = "Builder for [`DerivedRelation`](struct.DerivedRelation.html).\n"]
pub struct DerivedRelationBuilder {
    lateral: Option<bool>,
    subquery: Option<Box<ast::Query>>,
    alias: Option<ast::TableAlias>,
}
#[allow(clippy::all)]
#[allow(dead_code)]
impl DerivedRelationBuilder {
    #[allow(unused_mut)]
    pub fn lateral(&mut self, value: bool) -> &mut Self {
        let mut new = self;
        new.lateral = Option::Some(value);
        new
    }
    #[allow(unused_mut)]
    pub fn subquery(&mut self, value: Box<ast::Query>) -> &mut Self {
        let mut new = self;
        new.subquery = Option::Some(value);
        new
    }
    #[allow(unused_mut)]
    pub fn alias(&mut self, value: Option<ast::TableAlias>) -> &mut Self {
        let mut new = self;
        new.alias = value;
        new
    }
    #[doc = "Builds a new `DerivedRelation`.\n\n# Errors\n\nIf a required field has not been initialized.\n"]
    fn build(&self) -> Result<ast::TableFactor, BuilderError> {
        Ok(ast::TableFactor::Derived {
            lateral: match self.lateral {
                Some(ref value) => value.clone(),
                None => return Result::Err(Into::into(UninitializedFieldError::from("lateral"))),
            },
            subquery: match self.subquery {
                Some(ref value) => value.clone(),
                None => return Result::Err(Into::into(UninitializedFieldError::from("subquery"))),
            },
            alias: self.alias.clone(),
        })
    }
    #[doc = r" Create an empty builder, with all fields set to `None` or `PhantomData`."]
    fn create_empty() -> Self {
        Self {
            lateral: Default::default(),
            subquery: Default::default(),
            alias: Default::default(),
        }
    }
}
impl Default for DerivedRelationBuilder {
    fn default() -> Self {
        Self::create_empty()
    }
}

/// Runtime error when a `build()` method is called and one or more required fields
/// do not have a value.
#[derive(Debug, Clone)]
pub struct UninitializedFieldError(&'static str);

impl UninitializedFieldError {
    /// Create a new `UnitializedFieldError` for the specified field name.
    pub fn new(field_name: &'static str) -> Self {
        UninitializedFieldError(field_name)
    }

    /// Get the name of the first-declared field that wasn't initialized
    pub fn field_name(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for UninitializedFieldError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Field not initialized: {}", self.0)
    }
}

impl From<&'static str> for UninitializedFieldError {
    fn from(field_name: &'static str) -> Self {
        Self::new(field_name)
    }
}
impl std::error::Error for UninitializedFieldError {}

#[doc = "Error type for Builder"]
#[derive(Debug)]
#[non_exhaustive]
pub enum BuilderError {
    #[doc = r" Uninitialized field"]
    UninitializedField(&'static str),
    #[doc = r" Custom validation error"]
    ValidationError(String),
}
impl From<UninitializedFieldError> for BuilderError {
    fn from(s: UninitializedFieldError) -> Self {
        Self::UninitializedField(s.field_name())
    }
}
impl From<String> for BuilderError {
    fn from(s: String) -> Self {
        Self::ValidationError(s)
    }
}
impl fmt::Display for BuilderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UninitializedField(ref field) => write!(f, "`{}` must be initialized", field),
            Self::ValidationError(ref error) => write!(f, "{}", error),
        }
    }
}
impl std::error::Error for BuilderError {}
