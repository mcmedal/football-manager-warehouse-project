# Football Manager: Possession Data Extraction
This script extracts `Average Possession` data for various football clubs from a series of screenshot images. It uses Optical Character Recognition (OCR) to read the data from the images, processes the extracted text into a clean table, and then exports the final dataset to a single CSV file.

## 1. Setup: Imports and Environment
First, we import all the necessary libraries:
* **`os`**: To set environment variables.
* **`csv`**: For handling CSV files (though `pandas` is used for the final export).
* **`pandas`**: For creating and managing our data in DataFrames.
* **`easyocr`**: The main library for performing Optical Character Recognition.\n",
* **`re`**: For using regular expressions to parse the text.\n",
* **`PIL (Pillow)`**: For opening and processing images.\n",
* **`numpy`**: For handling image data as arrays.\n"

The `os.environ` line is a technical workaround to prevent errors related to duplicate libraries, which can sometimes occur in environments like Anaconda.
We also initialize the `easyocr.Reader`, specifying English (`'en'`) as the language to detect.


```python
# Importing necessary packages
import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'
import csv
import pandas as pd
import easyocr
import re
reader = easyocr.Reader(['en'])
from PIL import Image
import numpy as np
```

    Neither CUDA nor MPS are available - defaulting to CPU. Note: This module is much faster with a GPU.
    

## 2. Define Image File Paths
Here, we define lists of file paths for the images that contain the possession data. Each list corresponds to a different league/country, as the data was likely screenshotted in multiple parts.


```python
# Importing Average Possession Data
italy = [
    r"C:\...\FM data\italy 1.png",
    r"C:\...\FM data\italy 2.png"
]
germany = [
    r"C:\...\FM data\germany 1.png",
    r"C:\...\FM data\germany 2.png"
]
england = [
    r"C:\...\FM data\england 1.png",
    r"C:\...\FM data\england 2.png"
]
spain = [
    r"C:\...\FM data\spain 1.png",
    r"C:\...\FM data\spain 2.png"
]
portugal = [
    r"C:\...\FM data\portugal 1.png",
    r"C:\...\FM data\portugal 2.png"
]
belgium = [
    r"C:\...\FM data\belgium 1.png"
]
netherlands = [
    r"C:\...\FM data\netherlands 1.png",
    r"C:\...\FM data\netherlands 2.png"
]
england2 = [
    r"C:\...\FM data\england2 1.png",
    r"C:\...\FM data\england2 2.png"
]
france = [
    r"C:\...\FM data\france 1.png",
    r"C:\...\FM data\france 2.png"
]
```

## 3. Helper Functions
We define two functions to help us process the images and the text.
### Image Concatenation
This `img_concat` function is designed to import a list of images and concatenate them vertically. It also resizes all images to match the width of the *widest* image, ensuring they stack correctly. This is necessary to do, as my PC screen is not large enough to take the full screenshot.


```python
# To concatenate the reference images
def img_concat(img_list):
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

### Text Recognition and Parsing
The `text_recog` function takes the raw list of text strings from EasyOCR and parses it. It's designed to find pairs of (Club Name, Possession Value).
It works by:
1. Iterating through the list of detected text.
2. Using a regular expression (`re.search`) to find text that looks like a number (e.g., \"53.0\" or \"42%\").
3. If it finds a number, it assumes the *previous* text entry in the list was the club's name.
4. It filters out common header words (like `AVERAGE POSSESSION`) and any `club names` that don't start with a letter.
5. It returns a clean pandas DataFrame with `Club` and `Avg Poss` columns.


```python
# To recognize text within the images
def text_recog(ocr_list):
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

## 4. Run OCR and Process Data
Applying functions to the image lists

### Step 4a: Run OCR on Images
This converts each set of images into a single list of detected text.


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

### Step 4b: Parse OCR Results
This converts the unstructured text into clean DataFrames.


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

## 5. Combine and Inspect Data
All tables are concatenated into one single dataframe.


```python
# Concatenating all tables
poss_metric = pd.concat([italy, england, germany, spain, portugal, belgium, netherlands, england2, france], ignore_index = True)
poss_metric.head(10)
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
  </tbody>
</table>
</div>



## 6. Export Final Data to destination


```python
# Exporting to directory
poss_metric.to_csv(r"C:\...\FM data\possession_data.csv", encoding='utf-8', index = False)
```
