use sqlite::State;

fn main() {
    let connection = sqlite::open("file:data/mc_donalds.db").unwrap();

    let mut stores = Vec::new();

    let mut statement = connection
        .prepare("select identifier_value from us where sub_division=\"MA\"")
        .unwrap();

    while let State::Row = statement.next().unwrap() {
        stores.push(statement.read::<i64>(0).unwrap());
    }

    
}
