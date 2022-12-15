#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use fluvio_protocol::bytes::{BufMut, Buf};
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
use fluvio_controlplane_metadata::derivedstream::DerivedStreamSpec;
use fluvio_protocol::Version;

use crate::topic::TopicSpec;
use crate::customspu::CustomSpuSpec;
use crate::smartmodule::SmartModuleSpec;
use crate::tableformat::TableFormatSpec;
use crate::spg::SpuGroupSpec;
use crate::connector::ManagedConnectorSpec;

use crate::{AdminPublicApiKey, CreatableAdminSpec, Status};

#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateRequest<S: CreatableAdminSpec> {
    pub request: S,
}

/// Every create request must have this parameters
#[derive(Encoder, Decoder, Default, Debug)]
pub struct CommonCreateRequest {
    pub name: String,
    pub dry_run: bool,
    #[fluvio(min_version = 7)]
    pub timeout: Option<u32>, // timeout in milliseconds
}

impl Request for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const MIN_API_VERSION: i16 = 9;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = Status;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectApiCreateRequest {
    pub common: CommonCreateRequest,
    pub request: ObjectCreateRequest,
}

#[derive(Debug)]
pub enum ObjectCreateRequest {
    Topic(TopicSpec),
    CustomSpu(CustomSpuSpec),
    SmartModule(SmartModuleSpec),
    // SmartModuleDownload(SmartModuleDownloadSpec),
    ManagedConnector(ManagedConnectorSpec),
    SpuGroup(SpuGroupSpec),
    TableFormat(TableFormatSpec),
    DerivedStream(DerivedStreamSpec),
}

impl Default for ObjectCreateRequest {
    fn default() -> Self {
        Self::Topic(TopicSpec::default())
    }
}

impl ObjectCreateRequest {
    fn type_value(&self) -> u8 {
        match self {
            Self::Topic(_) => TopicSpec::CREATE_TYPE,
            Self::CustomSpu(_) => CustomSpuSpec::CREATE_TYPE,
            Self::SmartModule(_) => SmartModuleSpec::CREATE_TYPE,
            Self::ManagedConnector(_) => ManagedConnectorSpec::CREATE_TYPE,
            Self::SpuGroup(_) => SpuGroupSpec::CREATE_TYPE,
            Self::TableFormat(_) => TableFormatSpec::CREATE_TYPE,
            Self::DerivedStream(_) => DerivedStreamSpec::CREATE_TYPE,
        }
    }
}

impl Encoder for ObjectCreateRequest {
    fn write_size(&self, version: Version) -> usize {
        let type_size = (0u8).write_size(version);

        type_size
            + match self {
                Self::Topic(s) => s.write_size(version),
                Self::CustomSpu(s) => s.write_size(version),
                Self::SmartModule(s) => s.write_size(version),
                Self::ManagedConnector(s) => s.write_size(version),
                Self::SpuGroup(s) => s.write_size(version),
                Self::TableFormat(s) => s.write_size(version),
                Self::DerivedStream(s) => s.write_size(version),
            }
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), std::io::Error>
    where
        T: BufMut,
    {
        self.type_value().encode(dest, version)?;
        match self {
            Self::Topic(s) => s.encode(dest, version)?,
            Self::CustomSpu(s) => s.encode(dest, version)?,
            Self::ManagedConnector(s) => s.encode(dest, version)?,
            Self::SmartModule(s) => s.encode(dest, version)?,
            Self::SpuGroup(s) => s.encode(dest, version)?,
            Self::TableFormat(s) => s.encode(dest, version)?,
            Self::DerivedStream(s) => s.encode(dest, version)?,
        }

        Ok(())
    }
}

// We implement decode signature even thought this will be never called.
// RequestMessage use decode_object.  But in order to provide backward compatibility, we pretend
// to provide decode implementation but should be never called
impl Decoder for ObjectCreateRequest {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), std::io::Error>
    where
        T: Buf,
    {
        let typ = u8::decode_from(src, version)?;
        tracing::trace!("decoded type: {}", typ);

        let request = match typ {
            TopicSpec::CREATE_TYPE => {
                tracing::trace!("detected topic");
                let request = TopicSpec::decode_from(src, version)?;
                Self::Topic(request)
            }

            TableFormatSpec::CREATE_TYPE => {
                tracing::trace!("detected table");
                let request = TableFormatSpec::decode_from(src, version)?;
                Self::TableFormat(request)
            }

            CustomSpuSpec::CREATE_TYPE => {
                tracing::trace!("detected custom spu");
                let request = CustomSpuSpec::decode_from(src, version)?;
                Self::CustomSpu(request)
            }

            SpuGroupSpec::CREATE_TYPE => {
                tracing::trace!("detected custom spu");
                let request = SpuGroupSpec::decode_from(src, version)?;
                Self::SpuGroup(request)
            }

            SmartModuleSpec::CREATE_TYPE => {
                tracing::trace!("detected smartmodule");
                let request = SmartModuleSpec::decode_from(src, version)?;
                Self::SmartModule(request)
            }

            ManagedConnectorSpec::CREATE_TYPE => {
                tracing::trace!("detected connector");
                let request = ManagedConnectorSpec::decode_from(src, version)?;
                Self::ManagedConnector(request)
            }

            DerivedStreamSpec::CREATE_TYPE => {
                tracing::trace!("detected derivedstream");
                let request = DerivedStreamSpec::decode_from(src, version)?;
                Self::DerivedStream(request)
            }

            // Unexpected type
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid create type {:#?}", typ),
                ))
            }
        };
        *self = request;
        Ok(())
    }
}

/// Macro to convert create request
/// impl From<(CommonCreateRequest TopicSpec)> for ObjectApiCreateRequest {
/// fn from(req: (CommonCreateRequest TopicSpec)) -> Self {
///       ObjectApiCreateRequest {
///           common: req.0,
///           request: req.1
///       }
/// }
/// ObjectFrom!(WatchRequest, Topic);

macro_rules! CreateFrom {
    ($create:ty,$specTy:ident) => {
        impl From<(crate::objects::CommonCreateRequest, $create)>
            for crate::objects::ObjectApiCreateRequest
        {
            fn from(fr: (crate::objects::CommonCreateRequest, $create)) -> Self {
                crate::objects::ObjectApiCreateRequest {
                    common: fr.0,
                    request: crate::objects::ObjectCreateRequest::$specTy(fr.1),
                }
            }
        }
    };
}

pub(crate) use CreateFrom;

use super::COMMON_VERSION;
