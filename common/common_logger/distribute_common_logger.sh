#!/bin/bash

python -m build
echo "Start copying whl to target folders"

cp -f dist/common_logger-1.0.0-py3-none-any.whl ../../reports_dispatcher/
cp -f dist/common_logger-1.0.0-py3-none-any.whl ../../transactions_batch_loading/
cp -f dist/common_logger-1.0.0-py3-none-any.whl ../../transactions_file_generator/
cp -f dist/common_logger-1.0.0-py3-none-any.whl ../../transactions_producer/
cp -f dist/common_logger-1.0.0-py3-none-any.whl ../../transactions_stream_loading/

echo "Done!"
