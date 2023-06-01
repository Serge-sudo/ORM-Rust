#![forbid(unsafe_code)]

use crate::object::Schema;
use crate::storage::Row;
use crate::{
    data::ObjectId,
    error::{Error, NotFoundError, Result},
    object::Object,
    storage::StorageTransaction,
};
use std::ops::Deref;
use std::{
    any::{Any, TypeId},
    cell::{Cell, Ref, RefCell, RefMut},
    collections::HashMap,
    marker::PhantomData,
    rc::Rc,
};

////////////////////////////////////////////////////////////////////////////////

pub struct Transaction<'a> {
    cell_map: RefCell<HashMap<(TypeId, ObjectId), Rc<DataCell>>>,
    state_map: RefCell<StateMap>,
    inner: Box<dyn StorageTransaction + 'a>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(inner: Box<dyn StorageTransaction + 'a>) -> Self {
        Self {
            inner,
            cell_map: RefCell::default(),
            state_map: RefCell::default(),
        }
    }

    fn ensure_table<T: Object>(&self) -> Result<()> {
        if self.inner.table_exists(T::TABLE.table_name)? {
            return Ok(());
        }
        self.inner.create_table(T::TABLE)?;
        Ok(())
    }

    pub fn create<T: Object>(&self, src_obj: T) -> Result<Tx<'_, T>> {
        self.ensure_table::<T>()?;
        let map_key = (
            TypeId::of::<T>(),
            self.inner.insert_row(T::TABLE, &src_obj.serialize())?,
        );
        let cell = Rc::new(DataCell {
            id: map_key.1,
            content: RefCell::new(Box::new(src_obj)),
        });
        self.cell_map.borrow_mut().insert(map_key, cell.clone());
        let state = Rc::new(Cell::new(ObjectState::Clean));
        self.state_map.borrow_mut().insert(map_key, state.clone());
        Ok(Tx::new(cell, map_key.1, state, PhantomData))
    }

    pub fn get<T: Object>(&self, id: ObjectId) -> Result<Tx<'_, T>> {
        self.ensure_table::<T>()?;
        let map_key = (TypeId::of::<T>(), id);

        if let Some(state) = self.state_map.borrow().get(&map_key).cloned() {
            if let ObjectState::Removed = state.deref().get() {
                return Err(Error::NotFound(Box::new(NotFoundError::new(
                    id,
                    T::TABLE.type_name,
                ))));
            }
            if let Some(object) = self.cell_map.borrow().get(&map_key).cloned() {
                return Ok(Tx::new(object, id, state, PhantomData));
            }
        }

        let cell = Rc::new(DataCell {
            id,
            content: RefCell::new(Box::new(T::deserialize(
                self.inner.select_row(id, T::TABLE)?,
            ))),
        });
        self.cell_map.borrow_mut().insert(map_key, cell.clone());
        let state = Rc::new(Cell::new(ObjectState::Clean));
        self.state_map.borrow_mut().insert(map_key, state.clone());
        Ok(Tx::new(cell, id, state, PhantomData))
    }

    fn try_apply(&self) -> Result<()> {
        for (key, value) in self.cell_map.borrow().iter() {
            let object = value.content.borrow();
            let state = self.state_map.borrow().get(key).cloned().unwrap();
            match state.deref().get() {
                ObjectState::Removed => self.inner.delete_row(value.id, object.get_table())?,
                ObjectState::Modified => {
                    self.inner
                        .update_row(value.id, object.get_table(), &object.serialize())?
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.try_apply()?;
        self.inner.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        self.inner.rollback()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    Clean,
    Modified,
    Removed,
}

#[derive(Clone)]
pub struct Tx<'a, T> {
    cell: Rc<DataCell>,
    id: ObjectId,
    state: Rc<Cell<ObjectState>>,
    phantom: PhantomData<&'a T>,
}

impl<'a, T: Any> Tx<'a, T> {
    fn new(
        cell: Rc<DataCell>,
        id: ObjectId,
        state: Rc<Cell<ObjectState>>,
        phantom: PhantomData<&'a T>,
    ) -> Self {
        Self {
            cell,
            id,
            state,
            phantom,
        }
    }

    pub fn id(&self) -> ObjectId {
        self.id
    }

    pub fn state(&self) -> ObjectState {
        self.state.deref().get()
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        if let ObjectState::Removed = self.state.deref().get() {
            panic!("cannot borrow a removed object");
        } else {
            Ref::map(self.cell.content.borrow(), |store| {
                store.to_any().downcast_ref().unwrap()
            })
        }
    }

    pub fn borrow_mut(&self) -> RefMut<'_, T> {
        if let ObjectState::Removed = self.state.deref().get() {
            panic!("cannot borrow a removed object");
        } else {
            self.state.deref().set(ObjectState::Modified);
            RefMut::map(self.cell.content.borrow_mut(), |x| {
                x.to_any_mut().downcast_mut().unwrap()
            })
        }
    }

    pub fn delete(self) {
        if let Err(err) = self.cell.content.try_borrow_mut() {
            panic!("cannot delete a borrowed object {}", err);
        } else {
            self.state.deref().set(ObjectState::Removed);
        }
    }
}

pub trait Table {
    fn get_table(&self) -> &'static Schema;
}

impl<T: Object> Table for T {
    fn get_table(&self) -> &'static Schema {
        T::TABLE
    }
}

impl<T: Object> ToAny for T {
    fn to_any(&self) -> &dyn Any {
        self
    }

    fn to_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub trait ToAny {
    fn to_any(&self) -> &dyn Any;
    fn to_any_mut(&mut self) -> &mut dyn Any;
}

pub trait Serialize {
    fn serialize(&self) -> Row;
}

impl<T: Object> Serialize for T {
    fn serialize(&self) -> Row {
        self.serialize()
    }
}

pub trait Record: Table + Serialize + ToAny {}

impl<T: Object> Record for T {}

pub(crate) struct DataCell {
    pub(crate) id: ObjectId,
    pub(crate) content: RefCell<Box<dyn Record>>,
}

pub type StateMap = HashMap<(TypeId, ObjectId), Rc<Cell<ObjectState>>>;
