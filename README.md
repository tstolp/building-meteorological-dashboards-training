# training-weather-forecast-dashboards

![alt text](data/images/partners.svg)

This repository contains a Jupyter Notebook that guides you through the process of creating interactive dashboards in Python using TAHMO and Open-Meteo APIs. By following this tutorial, you will learn how to retrieve weather data from both services, process the data, and create an informative dashboard using Solara.

Ce référentiel contient un notebook Jupyter qui vous guide à travers le processus de création de tableaux de bord interactifs en Python en utilisant les API TAHMO et Open-Meteo. En suivant ce tutoriel, vous apprendrez comment récupérer des données météorologiques à partir des deux services, traiter les données et créer un tableau de bord informatif à l'aide de Solara.

![dashboard](https://github.com/tstolp/training-seasonal-forecasting/assets/33895313/d45b001d-8042-4523-846b-2770c6363548)

## Prerequisites - Prérequis
Before you begin, ensure you have the following:
- A GitHub account
- Access to GitHub Codespaces

Avant de commencer, assurez-vous de disposer des éléments suivants :
- Un compte GitHub
- Accès à GitHub Codespaces

### 1. Open GitHub Codespaces 
Clone the Repository:
Open the repository in GitHub Codespaces by clicking the "Code" button and selecting "Open with Codespaces."


Another option is to clone this repository and work locally using devcontainers

Une autre option consiste à cloner ce référentiel et à travailler localement en utilisant des devcontainers.

```bash
# Copy code
git clone https://github.com/tstolp/training-weather-forecast-dashboards.git
cd training-weather-forecast-dashboard
```

### 2. Run the Jupyter Notebook: - Exécutez le Notebook Jupyter :
Open the training-notebook.ipynb file and follow the step-by-step instructions to create your dashboard.

Ouvrez le fichier training-notebook.ipynb et suivez les instructions étape par étape pour créer votre tableau de bord.

### 3. Run using the Solara server

```bash
# Copy code
solara run sol.py --host=0.0.0.0
```

## Feedback and Contributions
If you have any feedback or suggestions, feel free to open an issue or create a pull request. Contributions are highly welcomed!

## License
This project is licensed under the MIT License - see the LICENSE file for details.

Happy dashboarding! 🌦️📊
