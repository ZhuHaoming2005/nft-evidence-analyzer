//! Resident entity identities, string pool, and CSR indexes.

pub mod csr;
pub mod ids;
pub mod store;
pub mod string_pool;

pub use csr::{CsrIndex, NamePostingStub, UriChainIndex, UriChainRun, UriPostingKey};
pub use ids::{
    ChainId, ChainTotals, Contract, ContractId, MetadataRecord, Nft, NftId, SourceOrder, StringId,
    compare_token_ids, compare_token_ids_desc, normalized_evm_token, normalized_evm_token_slice,
};
pub use store::{IdentityRow, ResidentStore, finalize_name_representatives_stub};
pub use string_pool::StringPool;
