use super::{
    Field,
    FieldSchema,
    RewindableIterator,
    TableIterator,
    TableSchema,
};

pub struct Select<'closure, Iter> {
    pub base: Iter,
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

pub struct CrossJoin<IterA, IterB> {
    iter_a: IterA,
    iter_b: IterB,

    current_a: Option<Vec<Field>>,
    schema: TableSchema,
}

fn concat_schemas(table_name: &str, sa: &TableSchema, sb: &TableSchema) -> TableSchema {
    let mut fields = Vec::with_capacity(sa.fields.len() + sb.fields.len());
    fields.extend(sa.fields.iter().map(|f| FieldSchema {
        name: format!("{}.{}", sa.name, f.name), ..*f
    }));
    fields.extend(sb.fields.iter().map(|f| FieldSchema {
        name: format!("{}.{}", sb.name, f.name),
        offset: f.offset + sa.entry_stride, ..*f
    }));

    TableSchema {
        name: table_name.to_owned(),
        fields: fields,
        entry_stride: sa.entry_stride + sb.entry_stride,
    }
}

pub fn cross<
    IterA: TableIterator,
    IterB: TableIterator + RewindableIterator<Vec<Field>>
>(mut iter_a: IterA, iter_b: IterB) -> CrossJoin<IterA, IterB> {
    let schema = concat_schemas("cross-join", iter_a.schema(), iter_b.schema());

    let first_a = iter_a.next();

    CrossJoin {
        iter_a: iter_a,
        iter_b: iter_b,

        current_a: first_a,
        schema: schema,
    }
}

impl<
    IterA: TableIterator,
    IterB: TableIterator + RewindableIterator<Vec<Field>>
> TableIterator for CrossJoin<IterA, IterB> {
    fn blocks_accessed(&self) -> uint {
        self.iter_a.blocks_accessed() + self.iter_b.blocks_accessed()
    }

    fn records_accessed(&self) -> uint {
        self.iter_a.records_accessed() + self.iter_b.records_accessed()
    }

    fn schema<'s>(&'s self) -> &'s TableSchema {
        &self.schema
    }
}

impl<
    IterA: TableIterator,
    IterB: TableIterator + RewindableIterator<Vec<Field>>
> Iterator<Vec<Field>> for CrossJoin<IterA, IterB> {
    fn next(&mut self) -> Option<Vec<Field>> {
        loop {
            return match self.current_a {
                None => None,
                Some(_) => match self.iter_b.next() {
                    None => {
                        self.current_a = self.iter_a.next();
                        self.iter_b.rewind();
                        continue;
                    },
                    Some(b) => Some(self.current_a.get_ref() + b),
                },
            };
        }
    }
}

pub struct SelectPrimaryKey<Iter> {
    pub base: Iter,
    pub key: Option<u32>,
}

impl<
    Iter: TableIterator + RandomAccessIterator<Vec<Field>>
> Iterator<Vec<Field>> for SelectPrimaryKey<Iter> {
    fn next(&mut self) -> Option<Vec<Field>> {
        match self.key {
            Some(k) => {
                self.key = None;
                self.base.idx(k as uint)
            },
            None => None,
        }
    }
}

impl<
    Iter: TableIterator + RandomAccessIterator<Vec<Field>>
> TableIterator for SelectPrimaryKey<Iter> {
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

pub struct PrimaryKeyJoin<'closure, IterA, IterB> {
    iter_a: IterA,
    iter_b: IterB,
    key_closure: |&Vec<Field>|:'closure -> Option<u32>,
    schema: TableSchema,
}

pub fn pk_join<
    'closure,
    IterA: TableIterator,
    IterB: TableIterator + RandomAccessIterator<Vec<Field>>
>(iter_a: IterA, iter_b: IterB, key_closure: |&Vec<Field>|:'closure -> Option<u32>)
        -> PrimaryKeyJoin<'closure, IterA, IterB> {
    let schema = concat_schemas("pk-join", iter_a.schema(), iter_b.schema());
    PrimaryKeyJoin {
        iter_a: iter_a,
        iter_b: iter_b,
        key_closure: key_closure,
        schema: schema,
    }
}

impl<
    'closure,
    IterA: TableIterator,
    IterB: TableIterator + RandomAccessIterator<Vec<Field>>
> Iterator<Vec<Field>> for PrimaryKeyJoin<'closure, IterA, IterB> {
    fn next(&mut self) -> Option<Vec<Field>> {
        loop {
            match self.iter_a.next() {
                None => return None,
                Some(val) => match (self.key_closure)(&val) {
                    None => continue,
                    Some(k) => match self.iter_b.idx(k as uint) {
                        None => continue,
                        Some(b) => return Some(val + b),
                    },
                },
            }
        }
    }
}

impl<
    'closure,
    IterA: TableIterator,
    IterB: TableIterator + RandomAccessIterator<Vec<Field>>
> TableIterator for PrimaryKeyJoin<'closure, IterA, IterB> {
    fn blocks_accessed(&self) -> uint {
        self.iter_a.blocks_accessed() + self.iter_b.blocks_accessed()
    }

    fn records_accessed(&self) -> uint {
        self.iter_a.records_accessed() + self.iter_b.records_accessed()
    }

    fn schema<'s>(&'s self) -> &'s TableSchema {
        &self.schema
    }
}
