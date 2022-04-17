import pandas as pd
import matplotlib.pyplot as plt
import re
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

x_name = "product_name"
y_name = "rating"
file_path = "H:\\project\\FlipkartReviewScrapper\\Output\\realme7_data.csv"
data = pd.read_csv(file_path)
df = pd.DataFrame(data=data)
if(y_name == 'price' and df[y_name].dtypes == 'object'):
    # price feature needs to replace symbol from it before plotting graph
    rem_symbol = lambda s: re.sub(r'[^\w]', '', s)
    df[y_name] = df[y_name].apply(rem_symbol)
    df[y_name] = df[y_name].astype('int64')

fig = Figure(figsize=(25, 15))
plt1 = fig.add_subplot(1, 3, 1)
xs = df[x_name]
ys = df[y_name]
#ys = [1,2,3,4,5]
plt1.scatter(xs, ys)
plt1.title.set_text("Product vs Ratings Scatter Plot")
plt1.set_xlabel("Product Names")
plt1.set_ylabel("Ratings")
plt2 = fig.add_subplot(1, 3, 2)
df_p = df.groupby(x_name).count().reset_index()
print(f"grouped df columns list - {df_p.columns}")
plt2.title.set_text("Percentage of Ratings per Product")
plt2.pie(df_p[y_name],labels = df_p[x_name],autopct='%1.1f%%',counterclock=False)
#plt2.pie(ys,labels = xs)
plt3 = fig.add_subplot(1, 3, 3)
plt3.title.set_text("Product vs Ratings Line Chart")
#plt3.hist(df_p[[y_name]])
for pr in df_p[[x_name]]:
    plt3.plot(df[df[x_name] == pr][y_name])
	
