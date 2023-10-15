from langchain.chat_models import ChatOpenAI

llm = ChatOpenAI()
q = "Who was the 16th president of the US?"
a = llm.predict(q)
print(a)
