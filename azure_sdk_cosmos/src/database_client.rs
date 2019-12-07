use crate::client2::{Client2, CosmosUriBuilder};
use crate::requests::ListCollectionsBuilder;
use crate::DatabaseTrait;

#[derive(Debug, Clone)]
pub struct DatabaseClient<'a, CUB>
where
    CUB: CosmosUriBuilder,
{
    pub(crate) client: &'a Client2<CUB>,
    database: &'a str,
}

impl<'a, CUB> DatabaseClient<'a, CUB>
where
    CUB: CosmosUriBuilder,
{
    pub(crate) fn new(client: &'a Client2<CUB>, database: &'a str) -> Self {
        DatabaseClient { client, database }
    }
}

impl<'a, CUB> DatabaseTrait<'a, CUB> for DatabaseClient<'a, CUB>
where
    CUB: CosmosUriBuilder,
{
    fn database(&self) -> &'a str {
        self.database
    }

    fn list(&'a self) -> ListCollectionsBuilder<'a, CUB> {
        ListCollectionsBuilder::new(self)
    }
}

impl<'a, CUB> DatabaseClient<'a, CUB> where CUB: CosmosUriBuilder {}
