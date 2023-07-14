# IngKart Project

#### Runtime folder 
INGKART_HOME=/home/userfolder/ingkart/

#### Binary folder where main py files.
$INGKART_HOME/bin or src

#### Logs folder where main py files.
$INGKART_HOME/logs/ task or pipelines

#### soruce folder where main py files.
$INGKART_HOME/source_files/

#### target folder where main py files.
$INGKART_HOME/target_files/
#### archive folder where main py files.
$INGKART_HOME/archive/


#### Confing folder path.json annd other code json files needed. <not job related json>
$INGKART_HOME/conf paths.json

#### Folders for json related to pipelins and task.
INGKART_REPO=$INGKART_HOME/repo

#### File naming format.
names of json file as to pipeline_name and task_name
all json files names has no whitespaces replace it with underscore (_) has to lowercase

#### Folder for program, project, pipelines, task and connction files
- $INGKART_REPO/programs   -> save the file as json program_name.json 
- $INGKART_REPO/programs/projects_names   -> create a project_folder and save the file as project_name.json
- $INGKART_REPO/programs/projects_names/pipelines  -> save the files as pipeline_name.json
- $INGKART_REPO/programs/projects_names/pipelines/tasks -> save the files as task_name.json
- $INGKART_REPO/connections -- connection names as to unique connection_name.json





