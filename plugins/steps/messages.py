from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='{7032637618:AAGhnWpoigGtZDAI3T0SD75VBvu8hTBb-Gk}',
                        chat_id='{4189987260}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{4189987260}',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
	# ваш код здесь #
    hook = TelegramHook(telegram_conn_id='test',
                        token='{7032637618:AAGhnWpoigGtZDAI3T0SD75VBvu8hTBb-Gk}',
                        chat_id='{4189987260}')
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    message = f'Исполнение дага {run_id} с {task_instance_key_str} прошло неуспешно'
    
    hook.send_message({
        'chat_id': '{4189987260}',
        'text': message
    })