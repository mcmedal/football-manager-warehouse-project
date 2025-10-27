```python
import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'
import csv
import pandas as pd
import easyocr
reader = easyocr.Reader(['en'])
from PIL import Image
import numpy as np
```

    Neither CUDA nor MPS are available - defaulting to CPU. Note: This module is much faster with a GPU.
    


```python
# Importing Average Possession Data
italy = [
    r"C:\Users\olufe\Documents\FM data\italy 1.png",
    r"C:\Users\olufe\Documents\FM data\italy 2.png"
]
germany = [
    r"C:\Users\olufe\Documents\FM data\germany 1.png",
    r"C:\Users\olufe\Documents\FM data\germany 2.png"
]
england = [
    r"C:\Users\olufe\Documents\FM data\england 1.png",
    r"C:\Users\olufe\Documents\FM data\england 2.png"
]
spain = [
    r"C:\Users\olufe\Documents\FM data\spain 1.png",
    r"C:\Users\olufe\Documents\FM data\spain 2.png"
]
portugal = [
    r"C:\Users\olufe\Documents\FM data\portugal 1.png",
    r"C:\Users\olufe\Documents\FM data\portugal 2.png"
]
belgium = [
    r"C:\Users\olufe\Documents\FM data\belgium 1.png"
]
netherlands = [
    r"C:\Users\olufe\Documents\FM data\netherlands 1.png",
    r"C:\Users\olufe\Documents\FM data\netherlands 2.png"
]
england2 = [
    r"C:\Users\olufe\Documents\FM data\england2 1.png",
    r"C:\Users\olufe\Documents\FM data\england2 2.png"
]
france = [
    r"C:\Users\olufe\Documents\FM data\france 1.png",
    r"C:\Users\olufe\Documents\FM data\france 2.png"
]
```


```python
# To concatenate the reference images
def img_concat(img_list):
    """
    Vertically concatenates images from a list of file paths.
    Resizes all images to match the largest width (maintaining aspect ratio).
    Returns a PIL Image object.
    """
    if not img_list:
        raise ValueError("img_list cannot be empty")

    # Open and convert all images to RGB
    imgs = [Image.open(p).convert("RGB") for p in img_list]

    # Find the largest width among the images
    max_width = max(img.width for img in imgs)

    # Resize each image proportionally to match the largest width
    resized_imgs = []
    for img in imgs:
        if img.width != max_width:
            # compute proportional height
            new_height = int((max_width / img.width) * img.height)
            img = img.resize((max_width, new_height))
        resized_imgs.append(np.asarray(img))

    # Vertically concatenate all images
    combined = np.concatenate(resized_imgs, axis=0)

    # Return the combined image as a PIL Image object
    return Image.fromarray(combined)
```


```python
def text_recog(ocr_list):
    """
    Parse OCR list for club names and possession values.
    Keeps only rows where club names start with a letter.
    Returns a DataFrame with columns ['Club', 'Avg Poss'].
    """
    if not isinstance(ocr_list, list) or len(ocr_list) < 2:
        return pd.DataFrame(columns=["Club", "Avg Poss"])

    rows = []
    headers = {"AVERAGE POSSESSION", "AVG", "POSS", "MATCHES", "PS €", "PAS %", "POS"}
    headers = {h.upper() for h in headers}

    for i, token in enumerate(ocr_list):
        s = str(token).strip()
        num_match = re.search(r"(\d{1,3}(?:\.\d+)?)", s)
        if not num_match:
            continue

        if "%" not in s and len(s) > 3:
            continue

        if i == 0:
            continue
        club_candidate = str(ocr_list[i - 1]).strip()
        if (
            club_candidate == "" 
            or club_candidate.upper() in headers 
            or not re.match(r"^[A-Za-z]", club_candidate)
        ):
            continue  # only keep if it starts with a letter

        num_str = num_match.group(1)
        try:
            poss_val = float(num_str)
        except ValueError:
            poss_val = float("nan")

        rows.append((club_candidate, poss_val))

    return pd.DataFrame(rows, columns=["Club", "Avg Poss"])
```


```python
# OCR
italy = reader.readtext(np.array(img_concat(italy)), detail = 0)
england = reader.readtext(np.array(img_concat(england)), detail = 0)
germany = reader.readtext(np.array(img_concat(germany)), detail = 0)
spain = reader.readtext(np.array(img_concat(spain)), detail = 0)
portugal = reader.readtext(np.array(img_concat(portugal)), detail = 0)
belgium = reader.readtext(np.array(img_concat(belgium)), detail = 0)
netherlands = reader.readtext(np.array(img_concat(netherlands)), detail = 0)
england2 = reader.readtext(np.array(img_concat(england2)), detail = 0)
france = reader.readtext(np.array(img_concat(france)), detail = 0)
```

    C:\ProgramData\anaconda3\Lib\site-packages\torch\utils\data\dataloader.py:666: UserWarning: 'pin_memory' argument is set as true but no accelerator is found, then device pinned memory won't be used.
      warnings.warn(warn_msg)
    


```python
# Applying the text recognition function to all Leagues
italy = text_recog(italy)
england = text_recog(england)
germany = text_recog(germany)
spain = text_recog(spain)
portugal = text_recog(portugal)
belgium = text_recog(belgium)
netherlands = text_recog(netherlands)
england2 = text_recog(england2)
france = text_recog(france)
```


```python
# Concatenating all tables
poss_metric = pd.concat([italy, england, germany, spain, portugal, belgium, netherlands, england2, france], ignore_index = True)
```


```python
poss_metric.head(50)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Club</th>
      <th>Avg Poss</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Lazio</td>
      <td>63.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Napoli</td>
      <td>61.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Milan</td>
      <td>57.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Juventus</td>
      <td>57.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Bologna</td>
      <td>55.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Roma</td>
      <td>55.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Blu-neri</td>
      <td>55.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Fiorentina</td>
      <td>52.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Como</td>
      <td>50.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Cagliari</td>
      <td>49.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Genoa</td>
      <td>48.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Parma</td>
      <td>46.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Udinese</td>
      <td>45.0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Sassuolo</td>
      <td>45.0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Atalanta</td>
      <td>44.0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Cremonese</td>
      <td>43.0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Hellas Verona</td>
      <td>43.0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Lecce</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Pisa</td>
      <td>41.0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Torino</td>
      <td>40.0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Man City</td>
      <td>59.0</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Arsenal</td>
      <td>58.0</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Man Utd</td>
      <td>57.0</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Liverpool</td>
      <td>56.0</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Aston Villa</td>
      <td>55.0</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Brighton</td>
      <td>54.0</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Chelsea</td>
      <td>53.0</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Burnley</td>
      <td>53.0</td>
    </tr>
    <tr>
      <th>28</th>
      <td>West Ham</td>
      <td>52.0</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Tottenham</td>
      <td>51.0</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Leeds Utd</td>
      <td>51.0</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Newcastle</td>
      <td>50.0</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Brentford</td>
      <td>50.0</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Wolves</td>
      <td>48.0</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Nottm Forest</td>
      <td>44.0</td>
    </tr>
    <tr>
      <th>35</th>
      <td>Fulham</td>
      <td>43.0</td>
    </tr>
    <tr>
      <th>36</th>
      <td>Sunderland</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>37</th>
      <td>Crystal Palace</td>
      <td>39.0</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Bournemouth</td>
      <td>37.0</td>
    </tr>
    <tr>
      <th>39</th>
      <td>Everton</td>
      <td>37.0</td>
    </tr>
    <tr>
      <th>40</th>
      <td>Dortmund</td>
      <td>59.0</td>
    </tr>
    <tr>
      <th>41</th>
      <td>Eintracht Frankfurt</td>
      <td>55.0</td>
    </tr>
    <tr>
      <th>42</th>
      <td>Bayern Munchen</td>
      <td>55.0</td>
    </tr>
    <tr>
      <th>43</th>
      <td>RB Leipzig</td>
      <td>55.0</td>
    </tr>
    <tr>
      <th>44</th>
      <td>FC Koln</td>
      <td>54.0</td>
    </tr>
    <tr>
      <th>45</th>
      <td>Freiburg</td>
      <td>54.0</td>
    </tr>
    <tr>
      <th>46</th>
      <td>Hamburg</td>
      <td>53.0</td>
    </tr>
    <tr>
      <th>47</th>
      <td>Stuttgart</td>
      <td>53.0</td>
    </tr>
    <tr>
      <th>48</th>
      <td>Wolfsburg</td>
      <td>52.0</td>
    </tr>
    <tr>
      <th>49</th>
      <td>Bayer Leverkusen</td>
      <td>52.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
len(set(poss_metric["Club"]))
```




    172




```python
set(poss_metric["Club"])
```




    {'AVS SAD',
     'AZ',
     'Ajax',
     'Alaves',
     'Alverca SAD',
     'Anderlecht',
     'Angers',
     'Antwerp',
     'Arouca',
     'Arsenal',
     'Aston Villa',
     'Atalanta',
     'Athletic Club',
     'Atletico de Madrid',
     'Augsburg',
     'Auxerre',
     'Barcelona',
     'Bayer Leverkusen',
     'Bayern Munchen',
     'Benfica',
     'Birmingham',
     'Blackburn',
     'Blu-neri',
     'Bologna',
     'Bournemouth',
     'Braga',
     'Brentford',
     'Brest',
     'Brighton',
     'Bristol City',
     'Burnley',
     'Cagliari',
     'Casa Pia',
     'Celta Vigo',
     'Cercle Brugge',
     'Charleroi',
     'Charlton',
     'Chelsea',
     'Club Brugge',
     'Como',
     'Coventry',
     'Cremonese',
     'Crystal Palace',
     'Derby County',
     'Dortmund',
     'Eintracht Frankfurt',
     'Elche',
     'Espanyol',
     'Estoril Praia',
     'Estrela da Amadora SAD',
     'Everton',
     'Excelsior',
     'FC Groningen',
     'FC Koln',
     'FC Twente',
     'FC Utrecht',
     'FC Volendam',
     'FCV Dender',
     'Famalicao',
     'Feyenoord',
     'Fiorentina',
     'Fortuna Sittard',
     'Freiburg',
     'Fulham',
     'Genk',
     'Genoa',
     'Gent',
     'Getafe',
     'Gil Vicente',
     'Girona',
     'Gladbach',
     'Go Ahead Eagles',
     'Hamburg',
     'Heidenheim',
     'Hellas Verona',
     'Heracles',
     'Hoffenheim',
     'Hull',
     'Ipswich Town',
     'Juventus',
     'Lazio',
     'Le Havre',
     'Lecce',
     'Leeds Utd',
     'Leicester',
     'Lens',
     'Levante',
     'Lille',
     'Liverpool',
     'Lorient',
     'Lyon',
     'Mainz 05',
     'Mallorca',
     'Man City',
     'Man Utd',
     'Marseille',
     'Mechelen',
     'Metz',
     'Middlesbrough',
     'Milan',
     'Millwall',
     'Monaco',
     'Moreirense',
     'NAC',
     'NEC',
     'Nacional',
     'Nantes',
     'Napoli',
     'Newcastle',
     'Nice',
     'Norwich',
     'Nottm Forest',
     'OH Leuven',
     'Osasuna',
     'Oxford United',
     'PEC Zwolle',
     'PSV Eindhoven',
     'Paris FC',
     'Paris Saint-Germain',
     'Parma',
     'Pisa',
     'Porto',
     'Portsmouth',
     'Preston',
     'QPR',
     'RAAL La Louviere',
     'RB Leipzig',
     'Rayo Vallecano',
     'Real Betis',
     'Real Madrid',
     'Real Oviedo',
     'Real Sociedad',
     'Rennes',
     'Rio',
     'Roma',
     'SC Telstar',
     'Santa Clara',
     'Sassuolo',
     'Sc Heerenveen',
     'Sevilla',
     'Sheffield United',
     'Sheffield Wednesday',
     'Sint-Truiden',
     'Southampton',
     'Sparta Rotterdam',
     'Sporting CP',
     'St Pauli',
     'Standard Liege',
     'Stoke',
     'Strasbourg',
     'Stuttgart',
     'Sunderland',
     'Swansea',
     'Tondela',
     'Torino',
     'Tottenham',
     'Toulouse',
     'Udinese',
     'Union Berlin',
     'Union Saint-Gilloise',
     'Valencia',
     'Villarreal',
     'Vitoria de Cuimaraes',
     'Watford',
     'Werder Bremen',
     'West Brom',
     'West Ham',
     'Westerlo',
     'Wolfsburg',
     'Wolves',
     'Wrexham',
     'Zulte Waregem'}




```python
#
poss_metric.at[129, "Club"] = "sc Heerenveen"
poss_metric.at[60, "Club"] = "Atlético de Madrid"
poss_metric.at[93, "Club"] = "Famalicão"
poss_metric.at[66, "Club"] = "Alavés"
poss_metric.at[110, "Club"] = "Standard Liège"
poss_metric.at[150, "Club"] = "Hull City"
poss_metric.at[44, "Club"] = "FC Köln"
poss_metric.at[86, "Club"] = "Rio Ave"
poss_metric.at[122, "Club"] = "N.E.C."
poss_metric.at[111, "Club"] = "RAAL La Louvière"
poss_metric.at[80, "Club"] = "Vitória de Guimarães"
```


```python
poss_metric.to_csv(r"C:\Users\olufe\Documents\FM data\exports\possession_data.csv", encoding='utf-8', index = False)
```


```python

```
