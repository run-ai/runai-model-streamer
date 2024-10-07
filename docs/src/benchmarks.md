# Run:ai Model Streamer Benchmarks

## Setup

* Hardware
  * Cloud provider: AWS
  * Machine type: g5.12xlarge
  * GPU: 4 x NVIDIA A10G
* Software:
  * OS: Ubuntu 22.04
  * Run:ai Model Streamer: 0.6.0
* Model:
  * Name: meta-llama/Meta-Llama-3-8B (https://huggingface.co/meta-llama/Meta-Llama-3-8B)
  * Precision: bfloat16
  * Format: 1 x Safetensor file

## Measurement
Loading the model to the GPU by running the Run:ai Model Streamer and copying the tensors to the GPU Memory

## Results

| **Storage** | **Time (s)** | Speed (GiB/s) |
|-------------|--------------|---------------|
| **AWS Gp3** | 14.36        | 1.03          |
| **AWS Io2** | 7.6          | 1.96          |
| **AWS S3**  | 4.24         | 3.51          |
