#![forbid(unsafe_code)]
use proc_macro::TokenStream;
use quote::quote;

use syn::{parse_macro_input, DeriveInput, Fields, Field, FieldsNamed, LitStr, DataStruct, Type};

#[proc_macro_derive(Object, attributes(table_name, column_name))]
pub fn derive_object(input: TokenStream) -> TokenStream {
    let DeriveInput { ident, data, attrs, .. } = parse_macro_input!(input);

    let tables = attrs
        .iter()
        .find(|attr| attr.path().is_ident("table_name"))
        .and_then(|attr| attr.parse_args::<LitStr>().ok().map(|lit_str| lit_str.value()))
        .unwrap_or_else(|| ident.to_string());

    let fields = if let syn::Data::Struct(DataStruct { fields: Fields::Named(FieldsNamed { named, .. }), .. }) = data {
        Some(named.into_iter().collect::<Vec<Field>>())
    } else {
        None
    };

    let (idents, columns, types) = if let Some(fields) = fields {
        fields.into_iter().map(|field| {
            let column = field.attrs
                .iter()
                .find(|attr| attr.path().is_ident("column_name"))
                .and_then(|attr| attr.parse_args::<LitStr>().ok().map(|lit_str| lit_str.value()))
                .unwrap_or_else(|| field.ident.as_ref().unwrap().to_string());

            (field.ident.unwrap(), column, field.ty)
        }).deal_out()
    } else {
        (Vec::new(), Vec::new(), Vec::new())
    };

    let schema_fields = idents.iter().zip(columns.iter()).zip(types.iter()).map(|((ident, column), ty)| {
        format!(
            "::orm::object::Column {{
                column_name: \"{}\",
                attr_name: stringify!({}),
                typ: <{} as ::orm::data::ObjectType>::TYPE,
            }}",
            column, ident, type_to_string(ty),
        )
    }).collect::<Vec<String>>().join(", ");

    let deserialize_fields = idents.iter().map(|ident| {
        format!("{}: iter.next().unwrap().into()", ident)
    }).collect::<Vec<String>>().join(", ");

    let serialize_fields = idents.iter().map(|ident| {
        format!("(&self.{}).into()", ident)
    }).collect::<Vec<String>>().join(", ");

    let expanded = format!(
        "impl ::orm::Object for {} {{
            const TABLE: &'static ::orm::object::Schema = &::orm::object::Schema {{
                table_name: \"{}\",
                type_name: stringify!({}),
                columns: &[{}],
            }};

            fn deserialize(row: ::orm::storage::Row) -> Self {{
                let mut iter = row.into_iter();
                Self {{
                    {}
                }}
            }}
            fn serialize(&self) -> ::orm::storage::Row {{
                let values = vec![{}];
                values.into()
            }}
        }}",
        ident, tables, ident, schema_fields, deserialize_fields, serialize_fields
    );

    expanded.parse().unwrap()
}


type DealerResult<A, B, C> = (Vec<A>, Vec<B>, Vec<C>);

trait Dealer {
    type A;
    type B;
    type C;

    fn deal_out(self) -> DealerResult<Self::A, Self::B, Self::C>;
}

impl<I, A, B, C> Dealer for I
    where
        I: Iterator<Item = (A, B, C)>,
{
    type A = A;
    type B = B;
    type C = C;

    fn deal_out(self) -> DealerResult<Self::A, Self::B, Self::C> {
        let (mut a, mut b, mut c) = (Vec::new(), Vec::new(), Vec::new());
        for (x, y, z) in self {
            a.push(x);
            b.push(y);
            c.push(z);
        }
        (a, b, c)
    }
}


fn type_to_string(ty: &Type) -> String {
    let tokens: TokenStream = TokenStream::from(quote! { #ty });
    tokens.to_string()
}