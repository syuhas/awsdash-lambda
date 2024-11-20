provider "aws" {
    region = "us-east-1"
}

terraform {
  backend "s3" {
    bucket = "terraform-lock-bucket"
    key    = "objectmanagers3/terraform.tfstate"
    region = "us-east-1"
    dynamodb_table = "terraform-lock-table"
  }
}

resource "aws_lambda_function" "update" {
  filename      = "lambda_function.zip"
  function_name = "ObjectManagerS3"
  role          = "arn:aws:iam::551796573889:role/lambdaAdmin"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.10"
  timeout       = 900
  memory_size   = 256
  source_code_hash = filebase64sha256("lambda_function.zip")
}