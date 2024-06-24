"""
This script copies Esri ASCII grid data from the input workspace, then re-projects to NZTM while clipping rasters by region 
The input region layer is an ArcGIS web service (Hosted Feature Layer in ArcGIS Online).

During the clipping, statistics and a world file are created. To keep those files together, they are added into a common zip file for a product
that includes all the files. Then, a Metadata JSON file is created for the product and stored in the zipped files' directory.

When a pair of zip/json files has been created, they are both uploaded to the specified S3 bucket into the --s3prefix folder
as long as boto3 is installed.

** Geospatial ETL workflow **

REQUIREMENTS:

arcpy (tested with ArcGIS Pro V3.3)
boto3 must be installed via ArcGIS Pro package manager (or: `pip install boto3`)
and a valid AWS credentials profile must be configured on the machine running this script.
--> if boto3 cannot be installed, the script skips over the upload part and will simply leave the zipped output in the output folder.


USAGE:

This script can be run from the command line with or without arguments, or from within ArcGIS Pro.
The easiest way to run it is as a standalone script, with no parameters (it will use the defaults configured below)

ArcGIS Pro usage:

1. Configure the script arguments (see below) to be used as default parameters in the tool 
2 .Create a new project
3. Add this script to the project (in a new ArcGIS Toolbox in Catalog)
4. Run the tool from the toolbox - some debugging Messages will shown in the tool dialog
5. The tool will create the zipped Shapefiles and Metadata JSON files, and upload them to the specified S3 bucket


INPUTS (hard-coded or passed as arguments):

: '-r', '--regions_layer', help='The hosted feature layer for regions to use (URL). (optional)', default=f'{FEATURE_SERVICE_URL}')
: '-f', '--files_input_folder', help='Provide the path to the folder where Esri ASCII grid files are stored. (optional)', default=f'{arcpy.env.workspace}')
: '-b', '--bucket_name', help='Name of the S3 bucket to upload files to (optional).', default=f'{COMPANY_BUCKET_DATA_HUB}')
: '-p', '--s3prefix', help='Prefix for the uploaded files (optional).', default=f'{PREFIX}')
: '-o', '--output_folder', help='Provide the path to the folder where the output zipped Shapefiles and Metadata JSON files will be stored. (optional)', default=f'{OUTPUT_FOLDER}')

Tilmann Steinmetz, 24 June 2024

"""

# Standard imports
import argparse
from datetime import datetime as datetimedatetime
import json
import os
import logging
import zipfile

# arcpy import
import arcpy

# Create logging
L = os.path.join(r"D:\Temp\logs", "Climatology-grids.log")
logging.basicConfig(
    filename=L,
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

logger.info("Starting")

# S3 bucket name
COMPANY_BUCKET_DATA_HUB = "COMPANY-data-hub"

# S3 folder within S3 bucket
PREFIX = "climatology-grids"


try:
    # try to import boto3 and create a session
    # deal with S3/AWS connection - but don't fail if the import isn't working
    import boto3
    from botocore.exceptions import ClientError

    # make connection to S3 bucket Replace # 'your_aws_profile' with the name of your AWS profile if you have one configured.
    #  If not, you can remove the # profile_name parameter, and Boto3 will use your default AWS credentials.
    session = boto3.Session()
    s3 = session.resource("s3")
    # session = boto3.Session(profile_name='your_aws_profile')

except ImportError:
    arcpy.AddWarning("Can't import boto3 - Files won't be uploaded to S3 Bucket.")
    logger.warning("Can't import boto3 - Files won't be uploaded to S3 Bucket.")
    pass

DEFAULT_FILENAME = rf"C:\temp\copy_esrigrid_to_geotiff_rename_and_clip_w_Arguments.py"

# Set the workspace environment
arcpy.env.workspace = "//COMPANY.local/ESRI_ASCII_data_grids"
arcpy.env.overwriteOutput = True

# Set the output folder for the compressed GeoTIFFs
OUTPUT_FOLDER_CONVERTED = r"D:\Temp\climate_raster_grids_export\converted"
OUTPUT_FOLDER_REGIONS = r"D:\Temp\climate_raster_grids_export\regions_grids"
OUTPUT_FOLDER_ZIPPED = r"D:\Temp\climate_raster_grids_export\regions_grids\zipped"

# Set the projection for the output GeoTIFFs
output_projection = arcpy.SpatialReference("GD_1949_New_Zealand_Map_Grid")


# Define earliest valid date (iso_date_str_start)
START_DATE_STR = "1991-01-01"
date_obj_start = datetimedatetime.strptime(START_DATE_STR, "%Y-%m-%d")
iso_date_str_start = date_obj_start.isoformat()

# Define last valid date (iso_date_str_stop)
STOP_DATE_STR = "2020-12-31"
date_obj_stop = datetimedatetime.strptime(STOP_DATE_STR, "%Y-%m-%d")
iso_date_str_stop = date_obj_stop.isoformat()

# Define the dictionary for month-name lookup
lookup_month_and_season_name = {
    "monthly1": "January",
    "monthly2": "February",
    "monthly3": "March",
    "monthly4": "April",
    "monthly5": "May",
    "monthly6": "June",
    "monthly7": "July",
    "monthly8": "August",
    "monthly9": "September",
    "monthly10": "October",
    "monthly11": "November",
    "monthly12": "December",
    "seasonal1": "Summer",
    "seasonal2": "Autumn",
    "seasonal3": "Winter",
    "seasonal4": "Spring",
    "annual": "Annual",
}

# Define the dictionary for parameter lookup (for the renaming of files)
lookup_dict_parameter = {
    "00": "Total-Rainfall",
    "01": "Wet-Days-GT-1mm",
    "02": "Mean-Air-Temperature",
    "03": "Mean-Daily-Maximum-Air-Temperature",
    "04": "Mean-Daily-Minimum-Air-Temperature",
    "09": "Total-Sunshine",
    "11": "Mean-Earth-Temperature-At-10cm",
    "17": "Mean-Daily-Global-Irradiance",
    "23": "Screen-Frost-Days",
    "33": "Mean-Daily-Wind-Speed-At-10m",
    "34": "Total-Penman-PET",
    "37": "Total-Growing-Degree-Days-GDD-base-5degC",
    "38": "Total-Growing-Degree-Days-GDD-base-10degC",
    "64": "Mean-9AM-RH",
    "68": "Total-Heating-Degree-Days-HDD-base-18degC",
    "74": "Days-Of-Soil-Moisture-Deficit",
    # Add more key-value pairs as needed
}

# Define the dictionary for region lookup (for the renaming of files)
lookup_dict_region = {
    "01": "Northland",
    "02": "Auckland",
    "03": "Waikato",
    "04": "Bay-Of-Plenty",
    "05": "Gisborne",
    "06": "Hawkes-Bay",
    "07": "Taranaki",
    "08": "Manawatu-Whanganui",
    "09": "Wellington",
    "12": "West-Coast",
    "13": "Canterbury",
    "14": "Otago",
    "15": "Southland",
    "16": "Tasman",
    "17": "Nelson",
    "18": "Marlborough",
    "99": "Chatham-Islands",
    # Add more key-value pairs as needed
}

# Set the URL for the ArcGIS Online feature service
FEATURE_SERVICE_URL = "https://services.arcgis.com/XTtANUDT8Va4DLwI/arcgis/rest/services/nz_regional_councils/FeatureServer/0"

# Make a feature layer from the ArcGIS Online feature service
feature_layer = arcpy.MakeFeatureLayer_management(FEATURE_SERVICE_URL, "feature_layer")

# Create a dictionary to store files with the same base name
ascfile_dict = {}
tif_dict = {}


# Iterate over all ASCII Grid files in the input folder and subfolders
def parse_input_files(input_location: str) -> dict:
    """Parse the input ASCII Grid files from input_location and create output folders. Create a dict with paths"""
    # Iterate over all ASCII Grid files in the input folder and subfolders
    for root, dirs, files in os.walk(input_location):
        for file in files:
            if file.endswith(".asc"):
                # Get the base name and extension
                base_name, ext = os.path.splitext(file)

                # Add the file to the dictionary
                if base_name in ascfile_dict:
                    ascfile_dict[base_name].append(os.path.join(root, file))
                else:
                    ascfile_dict[base_name] = [os.path.join(root, file)]

    # Create the output folders
    os.makedirs(OUTPUT_FOLDER_CONVERTED, exist_ok=True)
    logger.info(f"Created output folder: {OUTPUT_FOLDER_CONVERTED}")
    arcpy.AddMessage(f"Created output folder: {OUTPUT_FOLDER_CONVERTED}")
    os.makedirs(OUTPUT_FOLDER_REGIONS, exist_ok=True)
    logger.info(f"Created output folder: {OUTPUT_FOLDER_REGIONS}")
    arcpy.AddMessage(f"Created output folder: {OUTPUT_FOLDER_REGIONS}")
    os.makedirs(OUTPUT_FOLDER_ZIPPED, exist_ok=True)
    logger.info(f"Created output folder: {OUTPUT_FOLDER_ZIPPED}")
    arcpy.AddMessage(f"Created output folder: {OUTPUT_FOLDER_ZIPPED}")

    return ascfile_dict


# Process each group of files with the same base name, using the dictionary just created:
def process_files_in_ascdict(
    ascfile_dict: dict, COMPANY_bucket_data_hub: str, prefix: str
):
    # Process each group of files with the same base name
    for base_name, file_list in ascfile_dict.items():
        logger.info(f"Processing files with base name: {base_name}")
        """ Process the input ASCII grid files """
        for file_path in file_list:
            # Get the region code from the subfolder name
            region_code = os.path.basename(os.path.dirname(file_path))

            # Construct the input and output paths
            input_raster = file_path
            output_folder_converted_region = os.path.join(
                OUTPUT_FOLDER_REGIONS, region_code
            )
            os.makedirs(output_folder_converted_region, exist_ok=True)

            # Split the file name at the underscore characters
            parts = base_name.split("_")
            last_name = parts[-1].split(".")
            parameter_code = parts[1]

            month_and_season = lookup_month_and_season_name[parts[-1]]

            # Compose the new file name by looking up the codes/seasons in the dictionaries and replacing them with their values
            new_file_name = f"{lookup_dict_parameter[parameter_code]}_{parts[4]}_1991-2020_{month_and_season}"
            # print(new_file_name)

            output_raster = os.path.join(
                OUTPUT_FOLDER_CONVERTED, f"{new_file_name}.tif"
            )

            # Copy the raster and define the projection
            arcpy.CopyRaster_management(
                input_raster, output_raster, config_keyword="CLOUD_OPTIMIZED_GEOTIFF"
            )
            arcpy.DefineProjection_management(output_raster, output_projection)

            # Iterate over the features in the feature layer (to retrieve all regions for clipping individually)
            with arcpy.da.SearchCursor(
                feature_layer, ["REGC_code", "REGC_name_ascii", "SHAPE@"]
            ) as cursor:
                # Create a Spatial Reference object for the output projection (WGS84)
                output_sr = arcpy.SpatialReference(4326)
                for row in cursor:
                    region_code = row[0]
                    if region_code == "99":
                        continue
                    else:
                        region_name = lookup_dict_region[region_code]
                        # region_title is derived from row[1] in the cursor, but we strip "Region" from the string at the end:
                        region_title = row[1].split(" Region")[0]
                        if region_title.startswith("Area"):
                            region_title = "Chatham Islands"
                        feature_geometry = row[2]
                        region_extent = feature_geometry.extent

                        logger.info(
                            f"Processing region {region_name} with code {region_code} and title {region_title}"
                        )
                        arcpy.AddMessage(
                            f"Processing region {region_name} with code {region_code} and title {region_title}"
                        )

                        output_clipped_raster = os.path.join(
                            output_folder_converted_region,
                            f"{new_file_name.split('.')[0]}_{region_name}.tif",
                        )

                        """ Clip rasters. Use the arcpy.Clip_management tool for clipping region geometries out of the input rasters
                        - which does not require Spatial Analyst/Licensed Tool """
                        # run all in a with bracket
                        with arcpy.EnvManager(
                            outputCoordinateSystem='PROJCS["NZGD_2000_Transverse_Mercator",GEOGCS["GCS_NZGD_2000",DATUM["D_NZGD_2000",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",1600000.0],PARAMETER["False_Northing",10000000.0],PARAMETER["Central_Meridian",173.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]',
                            cellSize="MINOF",
                            geographicTransformations="New_Zealand_1949_To_NZGD_2000_3_NTv2",
                        ):
                            out_raster = arcpy.Clip_management(
                                in_raster=output_raster,
                                out_raster=output_clipped_raster,
                                in_template_dataset=feature_geometry,
                                nodata_value="NODATA",
                                clipping_geometry="ClippingGeometry",
                                maintain_clipping_extent="NO_MAINTAIN_EXTENT",
                            )
                            logger.info(
                                f"File {output_clipped_raster} with extent {region_extent} for region {region_name} created"
                            )

                            """ Find all files in the current folder which have the same basename and zip them up """
                            # get the directory, filename components from output_clipped_raster
                            base_clip_dir, base_clip_name = os.path.split(
                                output_clipped_raster
                            )

                            # Get the base name and extension
                            tif_base_name, tif_ext = os.path.splitext(base_clip_name)

                            zip_file_path = os.path.join(
                                OUTPUT_FOLDER_ZIPPED, f"{tif_base_name}.zip"
                            )
                            with zipfile.ZipFile(zip_file_path, "w") as zip_file:
                                # add all files with the same base name to the zip file
                                for file in os.listdir(base_clip_dir):
                                    if file.startswith(tif_base_name):
                                        if file.endswith(".lock"):
                                            # we don't want any trouble with lockfiles in zip containers
                                            pass
                                        else:
                                            try:
                                                zip_file.write(
                                                    os.path.join(base_clip_dir, file),
                                                    file,
                                                )
                                            except Exception as e:
                                                arcpy.AddError(
                                                    f"Error adding {file} to {zip_file_path}: {e}"
                                                )
                                                logger.error(
                                                    f"Error adding {file} to {zip_file_path}: {e}"
                                                )

                            logger.info(
                                f"Created {zip_file_path} for region {region_name}\n"
                            )
                            arcpy.AddMessage(
                                f"Created {zip_file_path} for region {region_name}\n"
                                f"Uploading to S3 bucket {COMPANY_bucket_data_hub}/{prefix}..."
                            )

                            upload_file(zip_file_path, COMPANY_bucket_data_hub, prefix)

                            """ Create metadata file (JSON format) after the zip file creation as we don't want to include it """
                            md_file = create_json_file(
                                output_clipped_raster,
                                prefix,
                                region_extent,
                                region_title,
                                month_and_season,
                            )
                            upload_file(md_file, COMPANY_bucket_data_hub, prefix)

    logger.info(
        f"Processing and upload to S3 bucket {COMPANY_bucket_data_hub} completed successfully."
    )
    arcpy.AddMessage(
        f"Processing and upload to S3 bucket {COMPANY_bucket_data_hub} (if boto3 is installed) completed successfully."
    )


# and upload both the Zip files and the JSON metadata files
def upload_file(file_path, bucket_name, prefix=None):
    """Upload a file to an S3 bucket

    :param file_path: Path to the file to upload
    :param bucket_name: Name of the S3 bucket
    :param prefix: S3 folder name (optional)
    :return: True if file was uploaded, else False
    """

    # Get the file name from the file path
    file_name = os.path.basename(file_path)

    # Construct the object name with the prefix (if provided)
    if prefix:
        object_name = f"{prefix.strip('/')}/{file_name}"
    else:
        object_name = file_name

    # Upload the file
    try:
        s3.Bucket(bucket_name).upload_file(file_path, object_name)
    except Exception as e:
        arcpy.AddWarning(e)
        arcpy.AddWarning(
            f"Error uploading {file_path} to S3 bucket {bucket_name}/{prefix}"
        )
        logger.error(e)
        logger.error(f"Error uploading {file_path} to S3 bucket {bucket_name}/{prefix}")
        return False
    return True


def create_json_file(
    file_path: str,
    prefix: str,
    extent: arcpy.Extent,
    region_title: str,
    month_and_season: str,
) -> str:
    """Use file naming convention and template to extract metadata information into a JSON file"""
    file_name = os.path.basename(file_path)
    file_stem = os.path.splitext(file_name)[0]
    components = file_name.split("_")
    # replace dash in region-name to make it easier to read
    type_param = "".join(char if char != "-" else " " for char in components[0])

    period = components[2]
    statistic = components[1]
    region = components[-1].split(".")[0]

    # # Define the input and output coordinate reference systems
    # input_crs = "epsg:2193"  # New Zealand Web Mercator

    # Define the input and output coordinate reference systems
    # input_crs = arcpy.SpatialReference(
    #     2193
    # )  # New Zealand Web Mercator (don't need to define this as it's taken from the extent polygon)
    output_crs = arcpy.SpatialReference(4326)  # WGS 84 (standard for GeoJSON)

    # Reproject the temporary feature class to the output coordinate system
    reproj_fc = arcpy.Project_management(
        extent.polygon, r"memory\temp_out_fc", output_crs
    )[0]

    # Extract the reprojected coordinates
    coordinates = []
    with arcpy.da.SearchCursor(reproj_fc, ["SHAPE@"]) as cursor:
        for row in cursor:
            coordinates.append([[coord.X, coord.Y] for coord in row[0].getPart(0)])

    # Clean up the temporary feature classes
    arcpy.Delete_management(reproj_fc)

    # Create the standard GeoJSON polygon, add info that CRS is NZTM (ESPG:2193)
    geojson = {"type": "Polygon", "coordinates": coordinates}

    # We need the ISO-formatted date/time with 'Z' appended
    iso_date_str_startz = iso_date_str_start + "Z"
    dateMin = {"dateMin": {"$date": iso_date_str_startz}}

    # We need the ISO-formatted date/time with 'Z' appended
    iso_date_str_stopz = iso_date_str_stop + "Z"
    dateMax = {"dateMax": {"$date": iso_date_str_stopz}}

    # Current time will be used for the updatedAt string in JSON
    now = datetimedatetime.now()

    # Format the datetime as a full ISO string
    iso_string = now.isoformat()

    # We need the ISO-formatted date/time with 'Z' appended
    iso_stringz = iso_string + "Z"
    up_at = {
        "updatedAt": {"$date": iso_stringz},
    }

    # Create the JSON data
    json_data = {
        "src": f"/{prefix}/{file_stem}.zip",
        "productRef": prefix,
        "metadata": {
            "title": f"Climatology Grid {type_param} (1991-2020), {month_and_season}, Region: {region_title}",
            "description": f"This dataset comprises a 500m resolution grid of climatologic normals (averages) for: Parameter: {type_param}; Statistic: {statistic}; Period: {period}; {month_and_season}; Region: {region_title}",
            "geojson": geojson,
            "dateMin": {"$date": iso_date_str_startz},
            "dateMax": {"$date": iso_date_str_stopz},
            "version": "1.0",
            "updatedAt": {"$date": iso_stringz},
            "parameter": type_param,
            "period": month_and_season,
            "statistic": statistic,
            "region": region_title,
        },
    }

    # get the directory, filename components from output_clipped_raster
    base_json_dir, base_json_file_name = os.path.split(file_path)
    json_file_name = os.path.splitext(base_json_file_name)[0] + ".json"
    # Create the JSON file
    json_file_path = os.path.join(OUTPUT_FOLDER_ZIPPED, json_file_name)
    with open(json_file_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)
        logger.info(f"Metadata created at {json_file_path}")
        arcpy.AddMessage(f"Metadata created at {json_file_path}")

    return json_file_path


def main():
    # Create the parser
    parser = argparse.ArgumentParser(
        description="A script to export climate ASCII grids/rasters from input folder into region-wise zip files & create metadata file (JSON). Upload to S3 bucket"
    )

    # Add positional arguments (required)
    default_filename = DEFAULT_FILENAME  # as configured at the top of the script
    # parser.add_argument('file_name', help='The name of the input script file (this script).')

    # Add optional arguments
    parser.add_argument(
        "-r",
        "--regions_layer",
        help="The hosted feature layer for regions to use (URL). (optional)",
        default=f"{FEATURE_SERVICE_URL}",
    )
    parser.add_argument(
        "-f",
        "--files_input_folder",
        help="Provide the path to the folder where ASCII gridfiles are stored. (optional)",
        default=f"{arcpy.env.workspace}",
    )
    # parser.add_argument('-p', '--s3prefix', help='Prefix for the uploaded files (optional).', default=f'{PREFIX}')
    parser.add_argument(
        "-b",
        "--bucket_name",
        help="Name of the S3 bucket to upload files to (optional).",
        default=f"{COMPANY_BUCKET_DATA_HUB}",
    )

    parser.add_argument(
        "-o",
        "--output_folder_zipped",
        help="Name of the output folder for zipped files (optional).",
        default=f"{OUTPUT_FOLDER_ZIPPED}",
    )

    # Parse the arguments
    args = parser.parse_args()

    file_name = default_filename
    # Access the arguments

    regions = args.regions_layer
    file_folder = args.files_input_folder
    # PREFIX = args.s3prefix
    bucket_name = args.bucket_name

    logger.info(f"File name: {file_name}")
    logger.info(f"Region: {regions}")
    logger.info(f"Folder with input files: {file_folder}\n")
    # logger.warning(f"Prefix for uploaded files: {PREFIX}\n")
    logger.info(f"S3 bucket name: {bucket_name}\n")
    logger.info(f"Output folder for zipped files: {OUTPUT_FOLDER_ZIPPED}\n")

    arcpy.AddMessage(f"File name: {file_name}")
    arcpy.AddMessage(f"Region: {regions}")
    arcpy.AddMessage(f"Folder with input files: {file_folder}\n")
    arcpy.AddMessage(f"Prefix for uploaded files: {PREFIX}\n")
    arcpy.AddMessage(f"S3 bucket name: {bucket_name}\n")
    arcpy.AddMessage(f"Output folder for zipped files: {OUTPUT_FOLDER_ZIPPED}\n")

    """ File export/upload script logic here: """
    # parse the inputs
    ascfile_dict = parse_input_files(file_folder)
    print(ascfile_dict)
    arcpy.AddMessage(ascfile_dict)

    # Process each group of files with the same base name, using the dictionary just created:
    process_files_in_ascdict(
        ascfile_dict=ascfile_dict, COMPANY_bucket_data_hub=bucket_name, prefix=PREFIX
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        arcpy.AddError(f"An error occurred: {e}")
