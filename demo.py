from openai import OpenAI
from mem0 import Memory
from mem0.configs.base import MemoryConfig
from mem0.vector_stores.configs import VectorStoreConfig
# openai_client = OpenAI()
openai_client= OpenAI(api_key="sk-FoGiPLX5XcHnK0cubvgWzg", base_url="http://proxy.openai.azure.sys.ctripcorp.com/v1")
# memory = Memory()
# 数据库连接配置
# DB_CONFIG = {
#     "host": "10.118.1.32",
#     "port": 5432,
#     "user": "flightbi",
#     "password": "CtripBt!@3",
#     "database": "fltaivectordb"
# }

def create_memory_with_pgvector():
    """创建使用PGvector的Memory实例"""

    # 配置向量存储
    vector_store_config = VectorStoreConfig(
        provider="pgvector",
        config={
            "dbname": "fltaivectordb",  # 修改为你的数据库名
            "user": "flightbi",  # 修改为你的用户名
            "password": "CtripBt!@3",  # 修改为你的密码
            "host": "10.118.1.32",  # 修改为你的主机
            "port": 5432,  # 修改为你的端口
            "collection_name": "mem0_memories",
            "embedding_model_dims": 1024,  # BGE-M3维度
            "hnsw": True,
            "diskann": False,
            "sslmode": "prefer"
        }
    )

    # # 配置embedding模型
    # embedder_config = BaseEmbedderConfig(
    #     provider="custom",
    #     config={
    #         "model": "bge-m3",
    #         "embedding_dims": 1024
    #     }
    # )

    # 创建Memory配置
    memory_config = MemoryConfig(
        vector_store=vector_store_config,
        # embedder=embedder_config,
        version="v1.1"
    )

    # 创建Memory实例
    memory = Memory(config=memory_config)

    return memory


# 替换原来的 memory = Memory()
memory = create_memory_with_pgvector()





def chat_with_memories(message: str, user_id: str = "default_user") -> str:
    # Retrieve relevant memories
    # breakpoint()
    relevant_memories = memory.search(query=message, user_id=user_id, limit=3)
    memories_str = "\n".join(f"- {entry['memory']}" for entry in relevant_memories["results"])

    # Generate Assistant response
    system_prompt = f"You are a helpful AI. Answer the question based on query and memories.\nUser Memories:\n{memories_str}"
    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": message}]
    response = openai_client.chat.completions.create(model="gpt-4o", messages=messages)
    assistant_response = response.choices[0].message.content

    # Create new memories from the conversation
    messages.append({"role": "assistant", "content": assistant_response})
    memory.add(messages, user_id=user_id)

    return assistant_response
    # return 0

def main():
    print("Chat with AI (type 'exit' to quit)")
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() == 'exit':
            print("Goodbye!")
            break
        print(f"AI: {chat_with_memories(user_input)}")

if __name__ == "__main__":
    main()