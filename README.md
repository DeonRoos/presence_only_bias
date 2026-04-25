# presence_only_bias

Species distribution modelling pipeline in Python using GBIF occurrence data, with a focus on presence-only sampling bias and neural network prediction.

## The problem

Biodiversity databases like GBIF record where species *were observed*, not where they *are absent*. Observers cluster around roads, urban areas, and popular recording sites, so a model trained naively on these records learns human behaviour as much as species ecology. This is the presence-only sampling bias problem.

## What this project does

1. Pulls species occurrence records from GBIF for a focal species in Aberdeenshire, Scotland
2. Extracts environmental covariates (climate, elevation, land cover) at each occurrence point
3. Generates pseudo-absences using two strategies: random background and environmentally stratified
4. Trains a neural network classifier and a logistic regression baseline
5. Predicts the species distribution across Aberdeenshire and maps the result
6. Compares model outputs under different pseudo-absence assumptions to demonstrate how bias propagates

## Why it matters

The presence-only problem is a specific instance of selection bias in training data, a problem that appears across commercial machine learning wherever data collection is not random. The methods used here (pseudo-absence generation, bias correction, model comparison) transfer directly to any domain where positive examples are abundant and true negatives are unknown.

## Requirements

See `environment.yml` for the full conda environment. Key dependencies: `pygbif`, `geopandas`, `rasterio`, `scikit-learn`, `matplotlib`.

## Usage

```bash
conda activate sdm
jupyter notebook
```

## Data

Occurrence data is fetched from GBIF at runtime and is not committed to this repository. Environmental rasters must be downloaded separately (see `data/README.md`).

## License

MIT
