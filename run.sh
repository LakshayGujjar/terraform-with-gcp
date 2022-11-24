#!/bin/bash
filename="/workspace/changed_dirs.txt"
echo "running run.sh"
while IFS= read -r p || [[ -n "$p" ]]; do
    pipeline="transformation/$p"
    if [ "$p" == "." ];then
        echo "No pipelines updated or added"
        break
    fi
    if [ "$p" = "setup.py" ] || [ "${p: -3}" != ".py" ];then
        echo "non python file or setup.py"
        continue
    fi
    echo "Template location will be gs://dummy_df_template/Template/${p:0:-3}DataflowTemplate"
    python3 "$pipeline" --template_location "gs://dummy_df_template/Template/${p:0:-3}DataflowTemplate"
done < "$filename"