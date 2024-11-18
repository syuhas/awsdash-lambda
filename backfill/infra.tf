provider "aws" {
    region = "us-east-1"
}

resource "aws_lambda_function" "backfill" {
  filename      = "lambda_function.zip"
  function_name = "BackfillDatabaseS3"
  role          = "arn:aws:iam::551796573889:role/lambdaAdmin"
  handler       = "lamda_function.lambda_handler"
  runtime       = "python3.10"
  timeout       = 60
  memory_size   = 128
  source_code_hash = filebase64sha256("lambda_function.zip")
}