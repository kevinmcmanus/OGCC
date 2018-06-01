# Ryan Greene
# IMPORT
import os
import shutil
import zipfile
import urllib.request
import requests
import time
import tablib
import pandas as pd
import geopandas as gpd
import fiona
#from simpledbf import Dbf5
from getpass import getpass
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

try:
    gis = GIS(profile = "COGCC_Nightly_Profile")
except:
    gis = GIS(url = "https://interfacegis.maps.arcgis.com/", username = input("Username: "), password = getpass(), profile = "COGCC_Nightly_Profile")

# LOG
def LOG(line):
    f = open(baseDir + LOGFolder + os.sep + "LOG.txt", "a")
    f.write(line)
    f.close()

# MAKE A CHECK FOR LOG FOLDER, DELETE LOG FILE
def checkLOG():
    if not os.path.isdir(baseDir + LOGFolder + os.sep):
        os.mkdir(baseDir + LOGFolder + os.sep)

    if os.path.isfile(baseDir + LOGFolder + os.sep + "LOG.txt"):
        os.remove(baseDir + LOGFolder + os.sep + "LOG.txt")

# MAKE A CHECK FOR ALL DIRECTORIES, IF THEY DON'T EXIST -> MAKE THEM
def checkLocalDir():
    LOG("Check local directory:\n\n")
    if os.path.isdir(baseDir + wellFilesFolder + os.sep):
        shutil.rmtree(baseDir + wellFilesFolder + os.sep)
    if os.path.isdir(baseDir + zipUpFolder + os.sep):
        shutil.rmtree(baseDir + zipUpFolder + os.sep)
    if os.path.isdir(baseDir + zipDownFolder + os.sep):
        shutil.rmtree(baseDir + zipDownFolder + os.sep)

    UpperDir = os.listdir(baseDir)
    DirList = [scriptsFolder, zipDownFolder, wellFilesFolder, zipUpFolder, LOGFolder]

    for folder in DirList:
        if folder in UpperDir:
            LOG("\t" + folder + " folder is in the local directory.\n")
        else:
            os.mkdir(baseDir + folder + os.sep)
            LOG("\t" + "Created " + folder + " folder in the local directory.\n")

    LOG("\n\tCOMPLETED: " + time.asctime() + "\n")

# DOWNLOAD ZIP FILES INTO ZipDown FOLDER
def download():
    LOG("\nDownload COGCC .zip files from http://cogcc.state.co.us into ZipDown folder:\n\n")
    for item in items:
        response = urllib.request.urlopen(items[item]["url"])
        meta = requests.head(items[item]["url"], allow_redirects=True)
        size = meta.headers.get('content-length', 0)
        
        if size is 0:
            continue

        fileDropZip = open(baseDir + zipDownFolder + os.sep + items[item]["fileIn"] + zipExtension, 'wb')
        shutil.copyfileobj(response, fileDropZip)
        LOG("\t" + items[item]["fileIn"] + " was downloaded.\n")

    fileDropZip.close()
    LOG("\n\tCOMPLETED: " + time.asctime() + "\n")

# UNZIP FILES AND MOVE THEM TO Well Files FOLDER
def unZip():
    LOG("\nUnzip GOGCC files into Well Files folder:\n\n")
    for item in items:
        unZip = zipfile.ZipFile(baseDir + zipDownFolder + os.sep + items[item]["fileIn"] + zipExtension)
#        unZip.extractall(baseDir + wellFilesFolder + os.sep + items[item]["fileOut"])
        unZip.extractall(baseDir + zipDownFolder + os.sep + items[item]["fileIn"])
        LOG("\t" + items[item]["fileOut"] + " was unzipped.\n")

    LOG("\n\tCOMPLETED: " + time.asctime() + "\n")

# ZIP FILES FROM Well Files FOLDER AND MOVE THEM TO ZipUP FOLDER
def reZip():
    LOG("\nZip GOGCC files into ZipUp folder:\n\n")
    for item in items:
#        shutil.make_archive(baseDir + zipUpFolder + os.sep + items[item]["fileOut"], "zip", baseDir + wellFilesFolder + os.sep + items[item]["fileOut"])
        shutil.make_archive(baseDir + zipUpFolder + os.sep + items[item]["fileOut"], "zip", baseDir + wellFilesFolder + os.sep + items[item]["name"])
        LOG("\t" + items[item]["fileOut"] + " was zipped.\n")

    LOG("\n\tCOMPLETED: " + time.asctime() + "\n")

# MANIPULATE INDIVIDUAL DBF FILES FROM EACH FOLDER (ADD LINK FIELD AND POPULATE)
def manipulate():
    LOG("\nEdit COGCC .dbf files in Well Files folder:\n\n")
    
    #get the facility status from the wells shapefile
    wells_sf_path = baseDir + zipDownFolder + os.sep + items['wells']["fileIn"]
    wells_sf = gpd.read_file(wells_sf_path)
    facility_status = wells_sf[['API','Facil_Stat']].copy()
    facility_status['API10'] = '05'+facility_status.API.str.slice(stop=8)
    #ditch the API column
    facility_status.drop('API',axis=1, inplace=True)


    for item in items:

        # input and output folders:
        sf_path_in  = baseDir + zipDownFolder + os.sep + items[item]["fileIn"]
        sf_path_out = baseDir + wellFilesFolder + os.sep + items[item]["name"]

        # path to the actual shape file:
        sf_path_in_full = sf_path_in + os.sep + items[item]['name'] + '.shp'

        if items[item]["fileOut"] is not "COGCC_PendPermit":
            # read up the shapefile
            gdf = gpd.read_file(sf_path_in)

            # get the schema:
            with fiona.open(sf_path_in_full) as f:
                input_schema = f.schema

            output_schema = input_schema
            outprops = output_schema['properties']

            # put on the API10 column
            gdf['API10'] = '05' + gdf.API.str.slice(stop = 8)
            outprops.update({'API10':'str:10'})

            # compute the links from the api numbers and tack the links onto the data frame
            gdf['Link'] = gdf.API.str.slice(stop = 8)\
                                     .apply(lambda s: 'http://cogcc.state.co.us/cogis/FacilityDetail.asp?facid={}&type=WELL'.format(s))
            outprops.update({'Link':'str:80'})

            # put the facility status on the DIRECTIONAL_BOTTOMHOLE_LOCATIONS and DIRECTIONAL_LINES files:
            if items[item]["fileOut"] is "COGCC_Directional" or items[item]["fileOut"] is "COGCC_BHL":
                gdf = gdf.merge(facility_status, on = 'API10', how='left')
                outprops.update({'Facil_Stat':'str:2'})


            # get the projection:
            prj_file = sf_path_in + os.sep + items[item]['name'] + '.prj'
            prj = [l.strip() for l in open(prj_file,'r')][0]

            # write it back out
            output_schema['properties'] = outprops
            gdf.to_file(sf_path_out, crs_wkt=prj, schema = output_schema)

            # copy the .sbn and sbx files from the source directory (sf_path_in) to the dest dir (sf_path_out)
            shutil.copy2(sf_path_in + os.sep + items[item]['name'] + '.sbn', sf_path_out)
            shutil.copy2(sf_path_in + os.sep + items[item]['name'] + '.sbx', sf_path_out)

        else: # Pending Permits: no facility detail link cuz there ain't no facility yet
            # just copy the shapefile contents to the dest dir
            shutil.copytree(sf_path_in, sf_path_out)
    
        LOG("\t" + items[item]["fileOut"] + " was manipulated.\n")

    LOG("\n\tCOMPLETED: " + time.asctime() + "\n")

# UPLOAD MANIPULATED FILES TO ARCGIS ONLINE
def uploadArc():
    LOG("\nUpload COGCC files to ArcGIS Online directory at: " + time.asctime() + "\n\n")
    foldername = 'Nightly COGCC'
    gis.content.create_folder(foldername)
    
    def add():
        time.sleep(5)
        gis.content.add(item_properties = {"type": "Shapefile"}, data = baseDir + zipUpFolder + os.sep + items[item]["fileOut"] + zipExtension, folder = foldername)
        LOG("\t" + items[item]["fileOut"] + " was uploaded.\n")
        
    def publish():
        time.sleep(5)
        file = gis.content.search(query = "title:{} AND type:{}".format(items[item]["fileOut"],"Shapefile"))
        file = file[0]
        file.publish(publish_parameters = {"name": items[item]["fileOut"] + "_published", "maxRecordCount": 2000}).layers[0]
        LOG("\t" + items[item]["fileOut"] + " was published.\n")
    
    # find file in list of files
    def findfile(files, filetitle):
        f = None
        for i in range(len(files)):
            if files[i].title == filetitle:
                f = files[i]
                break
                
        assert(f is not None)
        return f
      
    for item in items:
        file = gis.content.search(query = "title:{} AND type:{}".format(items[item]["fileOut"],"Shapefile"))
        file2 = gis.content.search(query = "title:{} AND type:{}".format(items[item]["fileOut"],"Feature"))
        if (len(file) is 0) or (len(file2) is 0):
            if len(file) is 0:
                add()
            if len(file2) is 0:
                publish()
        else:
            LOG("\toverwriting: " + items[item]["fileOut"] + "\n" )
            filelist = gis.content.search(query = "title:{} AND type:{}".format(items[item]["fileOut"],"Feature"))
            file = findfile(filelist, items[item]["fileOut"])
            newfile = FeatureLayerCollection.fromitem(file)
            newfile.manager.overwrite(baseDir + zipUpFolder + os.sep + items[item]["fileOut"] + zipExtension)
            LOG("\t" + items[item]["fileOut"] + " was overwritten.\n")
            
    LOG("\n\tCOMPLETED: " + time.asctime() + "\n")

# SET VARIABLES
scriptsFolder = "Scripts"
zipDownFolder = "ZipDown"
wellFilesFolder = "Well Files"
zipUpFolder = "ZipUp"
LOGFolder = "LOG"

baseDir = os.path.normpath(os.getcwd() + os.sep + os.pardir) + os.sep

zipExtension = ".ZIP"

items = {
    "directionalBottomholeLocations": {
        "url": "http://cogcc.state.co.us/documents/data/downloads/gis/DIRECTIONAL_BOTTOMHOLE_LOCATIONS_SHP.ZIP",
        "fileIn": "DIRECTIONAL_BOTTOMHOLE_LOCATIONS_SHP",
        "fileOut": "COGCC_BHL",
        "name": "Directional_Bottomhole_Locations"
    },
    "directionalLines": {
        "url": "http://cogcc.state.co.us/documents/data/downloads/gis/DIRECTIONAL_LINES_SHP.ZIP",
        "fileIn": "DIRECTIONAL_LINES_SHP",
        "fileOut": "COGCC_Directional",
        "name": "Directional_Lines"
    },
    "directionalLinesPending": {
        "url": "http://cogcc.state.co.us/documents/data/downloads/gis/DIRECTIONAL_LINES_PENDING_SHP.ZIP",
        "fileIn": "DIRECTIONAL_LINES_PENDING_SHP",
        "fileOut": "COGCC_PendLines",
        "name": "Directional_Lines_Pending"
    },
    "permits": {
        "url": "http://cogcc.state.co.us/documents/data/downloads/gis/PERMITS_SHP.ZIP",
        "fileIn": "PERMITS_SHP",
        "fileOut": "COGCC_Permit",
        "name": "Permits"
    },
    "permitsPending": {
        "url": "http://cogcc.state.co.us/documents/data/downloads/gis/PERMITS_PENDING_SHP.ZIP",
        "fileIn": "PERMITS_PENDING_SHP",
        "fileOut": "COGCC_PendPermit",
        "name": "Permits_Pending"
    },
    "wells": {
        "url": "http://cogcc.state.co.us/documents/data/downloads/gis/WELLS_SHP.ZIP",
        "fileIn": "WELLS_SHP",
        "fileOut": "COGCC_Wells",
        "name": "Wells"
    }
}

checkLOG()
checkLocalDir()
download()
unZip()
manipulate()
reZip()

uploadArc()

LOG("\nTask complete.")
