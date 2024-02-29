use crate::types::{
    Address, AuthorizedTransaction, Body, GetAddress, Transaction, Verify,
};
use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
use serde::{Deserialize, Serialize};

pub use ed25519_dalek::{
    Signature, SignatureError, Signer, SigningKey, Verifier, VerifyingKey,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ed25519_dalek error")]
    DalekError(#[from] SignatureError),
    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),
    #[error(
        "wrong key for address: address = {address},
             hash(verifying_key) = {hash_verifying_key}"
    )]
    WrongKeyForAddress {
        address: Address,
        hash_verifying_key: Address,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authorization {
    pub verifying_key: VerifyingKey,
    pub signature: Signature,
}

impl GetAddress for Authorization {
    fn get_address(&self) -> Address {
        get_address(&self.verifying_key)
    }
}

impl Verify for Authorization {
    type Error = Error;
    fn verify_transaction(
        transaction: &AuthorizedTransaction,
    ) -> Result<(), Self::Error> {
        verify_authorized_transaction(transaction)?;
        Ok(())
    }

    fn verify_body(body: &Body) -> Result<(), Self::Error> {
        verify_authorizations(body)?;
        Ok(())
    }
}

pub fn get_address(verifying_key: &VerifyingKey) -> Address {
    let mut hasher = blake3::Hasher::new();
    let mut reader = hasher.update(&verifying_key.to_bytes()).finalize_xof();
    let mut output: [u8; 20] = [0; 20];
    reader.fill(&mut output);
    Address(output)
}

struct Package<'a> {
    messages: Vec<&'a [u8]>,
    signatures: Vec<Signature>,
    verifying_keys: Vec<VerifyingKey>,
}

pub fn verify_authorized_transaction(
    transaction: &AuthorizedTransaction,
) -> Result<(), Error> {
    let serialized_transaction = bincode::serialize(&transaction.transaction)?;
    let messages: Vec<_> = std::iter::repeat(serialized_transaction.as_slice())
        .take(transaction.authorizations.len())
        .collect();
    let (verifying_keys, signatures): (Vec<VerifyingKey>, Vec<Signature>) =
        transaction
            .authorizations
            .iter()
            .map(
                |Authorization {
                     verifying_key,
                     signature,
                 }| (verifying_key, signature),
            )
            .unzip();
    ed25519_dalek::verify_batch(&messages, &signatures, &verifying_keys)?;
    Ok(())
}

pub fn verify_authorizations(body: &Body) -> Result<(), Error> {
    let input_numbers = body
        .transactions
        .iter()
        .map(|transaction| transaction.inputs.len());
    let serialized_transactions: Vec<Vec<u8>> = body
        .transactions
        .par_iter()
        .map(bincode::serialize)
        .collect::<Result<_, _>>()?;
    let serialized_transactions =
        serialized_transactions.iter().map(Vec::as_slice);
    let messages = input_numbers.zip(serialized_transactions).flat_map(
        |(input_number, serialized_transaction)| {
            std::iter::repeat(serialized_transaction).take(input_number)
        },
    );

    let pairs = body.authorizations.iter().zip(messages).collect::<Vec<_>>();

    let num_threads = rayon::current_num_threads();
    let num_authorizations = body.authorizations.len();
    let package_size = num_authorizations / num_threads;
    let mut packages: Vec<Package> = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
        let mut package = Package {
            messages: Vec::with_capacity(package_size),
            signatures: Vec::with_capacity(package_size),
            verifying_keys: Vec::with_capacity(package_size),
        };
        for (authorization, message) in
            &pairs[i * package_size..(i + 1) * package_size]
        {
            package.messages.push(*message);
            package.signatures.push(authorization.signature);
            package.verifying_keys.push(authorization.verifying_key);
        }
        packages.push(package);
    }
    for (authorization, message) in &pairs[num_threads * package_size..] {
        packages[num_threads - 1].messages.push(*message);
        packages[num_threads - 1]
            .signatures
            .push(authorization.signature);
        packages[num_threads - 1]
            .verifying_keys
            .push(authorization.verifying_key);
    }
    assert_eq!(
        packages.iter().map(|p| p.signatures.len()).sum::<usize>(),
        body.authorizations.len()
    );
    packages
        .par_iter()
        .map(
            |Package {
                 messages,
                 signatures,
                 verifying_keys,
             }| {
                ed25519_dalek::verify_batch(
                    messages,
                    signatures,
                    verifying_keys,
                )
            },
        )
        .collect::<Result<(), SignatureError>>()?;
    Ok(())
}

pub fn sign(
    signing_key: &SigningKey,
    transaction: &Transaction,
) -> Result<Signature, Error> {
    let message = bincode::serialize(&transaction)?;
    Ok(signing_key.sign(&message))
}

pub fn authorize(
    addresses_signing_keys: &[(Address, &SigningKey)],
    transaction: Transaction,
) -> Result<AuthorizedTransaction, Error> {
    let mut authorizations: Vec<Authorization> =
        Vec::with_capacity(addresses_signing_keys.len());
    let message = bincode::serialize(&transaction)?;
    for (address, keypair) in addresses_signing_keys {
        let hash_verifying_key = get_address(&keypair.verifying_key());
        if *address != hash_verifying_key {
            return Err(Error::WrongKeyForAddress {
                address: *address,
                hash_verifying_key,
            });
        }
        let authorization = Authorization {
            verifying_key: keypair.verifying_key(),
            signature: keypair.sign(&message),
        };
        authorizations.push(authorization);
    }
    Ok(AuthorizedTransaction {
        authorizations,
        transaction,
    })
}
