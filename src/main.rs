fn main() {
    let connection = sqlite::open("file:data/mc_donalds.db").unwrap();

    connection
    .execute(
        
    )
    .unwrap();
    println!("Hello, world!");
}
