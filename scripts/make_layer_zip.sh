# Description: Make the zip file for the layer

cd "$(dirname "$0")/.."
mkdir tmp_python
pip3 install --upgrade -r requirements.txt -t tmp_python/
zip -r layer.zip tmp_python
rm -r tmp_python/
