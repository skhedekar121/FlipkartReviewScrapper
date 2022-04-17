# doing necessary imports
import threading
import time 
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

from flask import Flask, render_template, request, jsonify, Response, url_for, redirect, send_file
# from flask import send_from_directory,  current_app,
from flask_cors import CORS, cross_origin
import pandas as pd
# from mongoDBOperations import MongoDBManagement
from FlipkratScrapping import FlipkratScrapper
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import configHandler as cfg
import cassandraOps as dbops,customLogger as lgr
# import os re
import re

rows = {}
# collection_name = None
product_name = None
free_status = True
file_path = ""
data_dict = dict()

app  = Flask(__name__)  # initialising the flask app with the name 'app'
clg  = lgr.customLogger(__name__)
ch   = cfg.configHandler("config.ini")

# Reading Config properties
# mongoOptions = ch.readConfigSection("mongodb")
# db_name    = mongoOptions['db_name']
# mongoUsr   = mongoOptions['user']
# mongoPwd   = mongoOptions['passwd']

output_folder = ch.readConfigOptions("output", "directory")

#For selenium driver implementation on heroku
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("disable-dev-shm-usage")

#To avoid the time out issue on heroku
class threadClass:

    def __init__(self, expected_review, searchString, scrapper_object, review_count,dbConn=None):
        self.expected_review = expected_review
        self.searchString = searchString
        self.scrapper_object = scrapper_object
        self.review_count = review_count
        self.dbConn = dbConn
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):
        global product_name, free_status
        free_status = False

        # product_name = self.scrapper_object.getReviewsToDisplay(expected_review=self.expected_review,
        #                                                         searchString=self.searchString, username=mongoUsr,
        #                                                         password=mongoPwd,
        #                                                         review_count=self.review_count)
        product_name = self.scrapper_object.getReviewsToDisplay(expected_review=self.expected_review,
                                                                   searchString=self.searchString,
                                                                   review_count=self.review_count,dbConn = self.dbConn) #Cassandra
        clg.log("Thread run completed for " + product_name)
        
        free_status = True



@app.route('/', methods=['POST', 'GET'])
@cross_origin()
def index():
    if request.method == 'POST':
        global free_status,file_path,product_name
        ## To maintain the internal server issue on heroku
        if free_status != True:
            return "This website is executing some process. Kindly try after some time..."
        else:
            free_status = True
        searchString = request.form['content'].replace(" ", "")  # obtaining the search string entered in the form
        expected_review = int(request.form['expected_review'])

        try:
            review_count = 0
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options,clg=clg)

            scrapper_object.openUrl("https://www.flipkart.com/")
            clg.log("Url hitted")
            scrapper_object.login_popup_handle()
            clg.log("login popup handled")
            scrapper_object.searchProduct(searchString=searchString)
            clg.log("Search begins for {} ".format(searchString))

            #mongoClient = MongoDBManagement(username=mongoUsr, password=mongoPwd)
            #if mongoClient.isCollectionPresent(collection_name=searchString, db_name=db_name):

            dbConn = dbops.cassandraOps(clg) #Cassandra
            if (dbConn.isConnected and dbConn.isTablePresent(searchString)): #Cassandra
                # response = mongoClient.findAllRecords(db_name=db_name, collection_name=searchString)
                # reviews = [i for i in response]

                reviews = dbConn.getListOfAllRecords(searchString) #Cassandra
                clg.log(f"In db have {len(reviews)} records")

                if len(reviews) >= expected_review:
                    result = [reviews[i] for i in range(0, expected_review)]
                    file_name = searchString + "_data.csv"
                    file_path = output_folder + "/" + file_name
                    scrapper_object.saveDataFrameToFile(dataframe=pd.DataFrame(result),file_path=file_path)

                    clg.log("Already exists enough Review data saved in db.....")
                    return render_template('results.html', rows=result,filepath=file_path,filename=file_name)  # show the results to user
                else:
                    review_count = len(reviews) 
                    threadClass(expected_review=expected_review, searchString=searchString,
                                scrapper_object=scrapper_object, review_count=review_count,dbConn= dbConn)
                    time.sleep(30)
                    clg.log("Redirecting to result page.....")
                    product_name = searchString


                    return redirect(url_for('feedback'))
            else:
                clg.log("Not having data in db.....Need to extract fresh")
                threadClass(expected_review=expected_review, searchString=searchString, scrapper_object=scrapper_object,
                            review_count=review_count,dbConn= dbConn)
                product_name = searchString
                time.sleep(30)
                return redirect(url_for('feedback'))

        except Exception as e:
            # raise Exception("(app.py) - Something went wrong while rendering all the details of product.\n" + str(e))
            msg = "(app.py) - Something went wrong while rendering all the details of product.\n" + str(e)
            clg.log(msg,"ERROR")
            return render_template('index.html',error=msg)
    else:
        return render_template('index.html')


@app.route('/feedback', methods=['GET'])
@cross_origin()
def feedback():
    global product_name, file_path

    try:

        clg.log('Product name - ' + str(product_name))

        if product_name is not None:
            reviews = None
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options,clg=clg)
            
            dbConn = dbops.cassandraOps(clg)  # Cassandra
            clg.log('Inside Feedback DB isConnected'+str(dbConn.isConnected))
            if dbConn.isConnected == True:
                reviews = dbConn.getListOfAllRecords(product_name)  # Cassandra
       
            # mongoClient = MongoDBManagement(username=mongoUsr, password=mongoPwd)
            # rows = mongoClient.findAllRecords(db_name=db_name, collection_name=product_name)
            # reviews = [i for i in rows]

            if(reviews is not None):

                clg.log("(feedback) Fetched all reviews ")
                dataframe = pd.DataFrame(reviews)
                file_name = product_name + "_data.csv"
                file_path = output_folder + "/" + file_name
                clg.log("(feedback) Storing Data Frame to {} ".format(file_path))

                scrapper_object.saveDataFrameToFile(dataframe=dataframe,file_path=file_path)
            else:

                msg = "(feedback) - Reviews result not found"
                clg.log(msg, "ERROR")

                return render_template('index.html', error=msg)
            # product_name = None
            return render_template('results.html', rows=reviews,filepath=file_path,filename=file_name)
        else:
            # return render_template('results.html', rows=None,filepath=None)
            msg = "(feedback) - Either DB not connected or Product not found"
            clg.log(msg, "ERROR")
            return render_template('index.html', error=msg)
    except Exception as e:
        # raise Exception("(feedback) - Something went wrong on retrieving feedback.\n" + str(e))
        msg = "(feedback) - Failed retrieving feedback.\n" + str(e)
        clg.log(msg, "ERROR")
        return render_template('index.html', error=msg)


@app.route("/graph", methods=['GET'])
@cross_origin()
def graph():
    return redirect(url_for('plot_png'))

@app.route('/a', methods=['GET'])
def plot_png():
    try:
        global file_path
        clg.log("(plot_png) file_path -> "+ file_path)
        fig = create_figure(file_path,'product_name','rating')

        if(fig != False):
            output = io.BytesIO()
            FigureCanvas(fig).print_png(output)
            clg.log("(plot_png) Displaying graph image")
            return Response(output.getvalue(), mimetype='image/png')
        
    except Exception as e:
        msg = "(plot_png) - Couldn't plot graph image \n" + str(e)
        clg.log(msg, "ERROR")
        return render_template('index.html', error=msg)

def create_figure(file_path,x_name,y_name):
    try:
        data = pd.read_csv(file_path)
        df = pd.DataFrame(data=data)

        clg.log("(create_figure) Scatter Plot Created between x {} and y {}".format(x_name, y_name))
        if(y_name == 'price' and df[y_name].dtypes == 'object'):
            # price feature needs to replace symbol from it before plotting graph
            rem_symbol = lambda s: re.sub(r'[^\w]', '', s)
            df[y_name] = df[y_name].apply(rem_symbol)
            df[y_name] = df[y_name].astype('int64')

        fig = Figure(figsize=(25, 15))
        plt1 = fig.add_subplot(1, 2, 1)
        xs = df[x_name]
        ys = df[y_name]
        #ys = [1,2,3,4,5]
        plt1.scatter(xs, ys)
        plt1.title.set_text("Product vs Ratings Scatter Plot")
        plt1.set_xlabel("Product Names")
        plt1.set_ylabel("Ratings")
        plt2 = fig.add_subplot(1, 2, 2)
        df_p = df.groupby(x_name).count().reset_index()
        clg.log(f"grouped df columns list - {df_p.columns}")
        plt2.title.set_text("Percentage of Ratings per Product")
        plt2.pie(df_p[y_name],labels = df_p[x_name],autopct='%1.1f%%',counterclock=False)
        #plt2.pie(ys,labels = xs)
        #plt3 = fig.add_subplot(1, 3, 3)
        #plt3.title.set_text("Product vs Ratings Line Chart")
        #plt3.hist(df_p[[y_name]])
        #for pr in df_p[[x_name]]:
            #plt3.plot(df[df[x_name] == pr][y_name])
	
        return fig

    except Exception as e:
        msg = "(create_figure) - Couldn't create graph \n" + str(e)
        clg.log(msg, "ERROR")
        return False

@app.route('/download/<path:filename>', methods=['GET', 'POST'])
def download(filename):
    global file_path
    try:
        # global output_folder
        # downloads = os.path.join(current_app.root_path, output_folder)
        # return send_from_directory(directory=downloads, filename=filename)
        if(filename in file_path):
            clg.log("(download) Downloaded file"+filename)
            return send_file(file_path, as_attachment=True)
        else:
            msg = "(download) Failed to download file "
            clg.log(msg, "ERROR")
            return render_template('index.html', error=msg)
    except:
        msg = "(download) Something Failed while downloading file "
        clg.log(msg, "ERROR")
        return render_template('index.html', error=msg)

if __name__ == "__main__":
    app.run()  # running the app on the local machine on port 8000
