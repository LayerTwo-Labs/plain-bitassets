//! Schemas for OpenAPI

use std::marker::PhantomData;

use utoipa::{
    openapi::{self, RefOr, Schema},
    PartialSchema, ToSchema,
};

/// Array of `T`s
pub struct Array<T>(PhantomData<T>);

impl<T> PartialSchema for Array<T>
where
    T: PartialSchema,
{
    fn schema() -> RefOr<Schema> {
        use openapi::schema::ToArray;
        T::schema().to_array().into()
    }
}

/// Utoipa does not support tuples at all, so these are represented as an
/// arbitrary json value
pub struct ArrayTuple<A, B>(PhantomData<A>, PhantomData<B>);

impl<A, B> PartialSchema for ArrayTuple<A, B>
where
    A: PartialSchema,
    B: PartialSchema,
{
    fn schema() -> RefOr<Schema> {
        openapi::schema::AllOf::builder()
            .item(A::schema())
            .item(B::schema())
            .to_array_builder()
            .build()
            .into()
    }
}

/// Array representation of a triple
pub struct ArrayTuple3<A, B, C>(PhantomData<A>, PhantomData<B>, PhantomData<C>);

impl<A, B, C> PartialSchema for ArrayTuple3<A, B, C>
where
    A: PartialSchema,
    B: PartialSchema,
    C: PartialSchema,
{
    fn schema() -> RefOr<Schema> {
        openapi::schema::AllOf::builder()
            .item(A::schema())
            .item(B::schema())
            .item(C::schema())
            .to_array_builder()
            .build()
            .into()
    }
}

pub struct BitcoinTxid;

impl PartialSchema for BitcoinTxid {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(openapi::Type::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema for BitcoinTxid {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("bitcoin.Txid")
    }
}

pub struct Fraction;

impl PartialSchema for Fraction {
    fn schema() -> RefOr<Schema> {
        utoipa::openapi::Object::new().into()
    }
}

impl ToSchema for Fraction {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Fraction")
    }
}

pub struct OpenApi;

impl PartialSchema for OpenApi {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::new();
        RefOr::T(Schema::Object(obj))
    }
}

/// Optional `T`
pub struct Optional<T>(PhantomData<T>);

impl<T> PartialSchema for Optional<T>
where
    T: PartialSchema,
{
    fn schema() -> openapi::RefOr<openapi::schema::Schema> {
        openapi::schema::OneOf::builder()
            .item(
                openapi::schema::Object::builder()
                    .schema_type(openapi::schema::Type::Null),
            )
            .item(T::schema())
            .into()
    }
}
