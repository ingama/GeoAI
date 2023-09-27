import pandas as pd
from sklearn.preprocessing import OrdinalEncoder
from sklearn.preprocessing import OneHotEncoder

#define data
categories = [["frost"],["freeze"],["hard freeze"]]
wdf = pd.DataFrame(categories, columns=["Weather"],  index=["bad", "worse", "worst"])
display(wdf)

#ordinal encoding
wdf_o = wdf.copy()
encoder = OrdinalEncoder()
wdf_o["Encoded"] = encoder.fit_transform(wdf_o)
display(wdf_o)

#one-hot encoding
categories = [["frost"],["freeze"],["hard freeze"]]
encoder = OneHotEncoder(categories='auto', sparse_output=False)
encoded_data = encoder.fit_transform(wdf)
wdf_h = pd.DataFrame(encoded_data, columns=encoder.categories_,  index= wdf.index)
display(wdf_h)