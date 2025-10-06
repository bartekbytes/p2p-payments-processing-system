python -m build
echo Start copying whl to target folders

copy /y dist\common_models-1.0.0-py3-none-any.whl ..\..\transactions_batch_loading\
copy /y dist\common_models-1.0.0-py3-none-any.whl ..\..\transactions_file_generator\
copy /y dist\common_models-1.0.0-py3-none-any.whl ..\..\transactions_producer\
copy /y dist\common_models-1.0.0-py3-none-any.whl ..\..\transactions_stream_loading\

echo Done!