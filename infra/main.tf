provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "raw" {
  bucket = "eia-general_dataset-raw"
}

resource "aws_s3_bucket" "processed" {
  bucket = "eia-general_dataset-processed"
}
