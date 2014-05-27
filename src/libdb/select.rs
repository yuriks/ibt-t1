use super::{TableIterator, TableSchema, Field};

/*
enum Expression<FieldRef> {
    ConstantExpr(Field),
    FieldExpr(FieldRef),
}

enum Condition<FieldRef> {
    TrueCond,
    FalseCond,
    AndCond(Box<Condition<FieldRef>>, Box<Condition<FieldRef>>),
    OrCond(Box<Condition<FieldRef>>, Box<Condition<FieldRef>>),

    EqualCond(Expression<FieldRef>, Expression<FieldRef>),
}
*/

pub struct Select<'closure, Iter> {
    pub base: Iter,
    //condition: Condition<uint>,
    pub condition: |&Vec<Field>|:'closure -> bool,
}

impl<'closure, Iter: TableIterator> Iterator<Vec<Field>> for Select<'closure, Iter> {
    fn next(&mut self) -> Option<Vec<Field>> {
        loop {
            match self.base.next() {
                None => return None,
                Some(val) =>
                    if (self.condition)(&val) { return Some(val) }
                    else { continue; }
            }
        }
    }
}

impl<'closure, Iter: TableIterator> TableIterator for Select<'closure, Iter> {
    fn blocks_accessed(&self) -> uint {
        self.base.blocks_accessed()
    }

    fn records_accessed(&self) -> uint {
        self.base.records_accessed()
    }

    fn schema<'s>(&'s self) -> &'s TableSchema {
        self.base.schema()
    }
}
