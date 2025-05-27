use tokio::io::AsyncWriteExt;

pub async fn write_to_stdout(text: &str) -> Result<(), Box<dyn ::std::error::Error>> {
    let mut stdout = tokio::io::stdout();
    stdout.write(text.as_bytes()).await?;
    stdout.flush().await?;
    Ok(())
}
