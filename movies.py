import kagglehub

# Download latest version
path = kagglehub.dataset_download("asaniczka/tmdb-movies-dataset-2023-930k-movies", output_dir="datasets")

print("Path to dataset files:", path)