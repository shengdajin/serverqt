#include <stdio.h>
#include <stdlib.h>   
#include <string.h> 
#include <stdint.h>   //uint8
#include <errno.h>  
#include <fcntl.h>   
#include <mysql/mysql.h> 
#include <sys/socket.h> 
#include <arpa/inet.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/epoll.h>

//常量定义
#define MAX_EVENTS 1024 //epoll一次最多处理的事件数
#define MAX_CLIENTS 100 //最大客户端连接数
#define BUFFER_SIZE 4096 //通用缓存区大小
#define MY_PROTOCOOL_VERSION 1 //当前协议版本
#define MAX_FILENAME_LEN 256  //最大文件名长度


//消息类型
#define MSG_TYPE_TEXT 1 //文本消息
#define MSG_TYPE_FILE 2 //文件消息

//协议包头
typedef struct {
    uint8_t  version;//1字节
    uint8_t  msg_type;//1字节
	uint32_t data_len;//4字节 当前数据包长度
	uint32_t filename_len;//4字节
    uint64_t file_size;//8字节 整个文件大小
	uint64_t sSum;//8字节
} PacketHeader;

//客户端连接信息
typedef struct {
    int socket;
    int id;
    char current_filename[MAX_FILENAME_LEN];//当前文件名
    int file_fd;
    size_t received_size; // 已接收的文件数据大小
} Client;


char sendbuffer[BUFFER_SIZE];
char recvbuffer[BUFFER_SIZE];

//网络字节序
uint64_t htonll(uint64_t value) {
    return ((uint64_t)htonl((uint32_t)(value >> 32)) << 32) | htonl((uint32_t)value);
}

//主机字节序
uint64_t ntohll(uint64_t value) {
    return ((uint64_t)ntohl((uint32_t)(value >> 32)) << 32) | ntohl((uint32_t)value);
}

//错误处理函数
void error_exit(const char* msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

//设置文件为非阻塞模式 
void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL); //F_GETFL获取文件标识符
    if (flags == -1) error_exit("fcntl(F_GETFL)");
    flags |= O_NONBLOCK; //设置非阻塞 异步
    if (fcntl(fd, F_SETFL, flags) == -1) error_exit("fcntl(F_SETFL)");//F_SETFL 设置文件标识符
}

// 生成唯一ID并插入客户端信息（带事务保护）
bool insert_client_with_id(MYSQL* conn, const char* ip, int port, int* client_id) {
    char escaped_ip[BUFFER_SIZE] = { 0 };
    // 转义 IP 地址
    mysql_real_escape_string(conn, escaped_ip, ip, strlen(ip));

    // 开启事务
    if (mysql_query(conn, "START TRANSACTION") != 0) {
        fprintf(stderr, "开启事务失败: %s\n", mysql_error(conn));
        return false;
    }

    // 构建并执行插入语句
    char query[BUFFER_SIZE] = { 0 };
    snprintf(query, sizeof(query), "INSERT INTO clients (ip, port) VALUES ('%s', %d)", escaped_ip, port);
    if (mysql_query(conn, query) != 0) {
        fprintf(stderr, "插入失败: %s\n", mysql_error(conn));
        if (mysql_query(conn, "ROLLBACK") != 0) {
            fprintf(stderr, "回滚失败: %s\n", mysql_error(conn));
        }
        return false;
    }

    // 获取数据库自动生成的自增 ID
    *client_id = mysql_insert_id(conn);

    // 提交事务
    if (mysql_query(conn, "COMMIT") != 0) {
        fprintf(stderr, "提交事务失败: %s\n", mysql_error(conn));
        return false;
    }

    return true;
}

// 计算校验和
uint64_t calculate_checksum(const char* data, size_t len) {
    uint64_t sum = 0;

    for (size_t i = 0; i < len; i++) {
        sum += (uint8_t)data[i];  // 将每个字节转换为无符号8位整数并累加
    }

    return sum;
}

//文本处理
void handle_text_message(Client clients[], int client_fd, PacketHeader header, uint32_t headchecksum) {
    //uint32_t len = header.filename_len;
    //分配缓冲区
    char buffer[BUFFER_SIZE];
    //转换主机字节序(main函数已经转了)
    uint32_t datalen = header.data_len;
    if (data_len >= BUFFER_SIZE) {
        fprintf(stderr, "文本消息过长\n");
        return;
    }
    //将接收到数据发送到缓冲区（data)
    ssize_t bytes_read = recv(client_fd, buffer, header.data_len, 0);
    //检查接受完整性
    if (bytes_read != header.data_len) {
        fprintf(stderr, "文本消息接收不完整\n");
        return;
    }
    uint64_t textchecksum = calculate_checksum(buffer, sizeof(bytes_read));
    uint64_t totalchecksum = headchecksum + textchecksum;
    if (textchecksum != header.sSum) {
        fprintf(stderr, "校验不完整(计算值：%llu 接受值：%llu)\n",(unsigned long long)textchecksum, (unsigned long long)header.sSum);
        return;
    }
    buffer[bytes_read] = '\0';//添加结束符
    //收到文本消息打印出来
    printf("收到文本消息: %s\n", buffer);
    broadcast_text_message(clients, client_fd, header, buffer, bytes_read, MSG_TYPE_TEXT, NULL, 0, 0, totalchecksum);
}

//文本广播发送
void broadcast_text_message(Client clients[], int sender_fd, PacketHeader header, const void* data, size_t len,
    uint8_t msg_type, const char* filename, uint32_t filenamelen, uint64_t filesize,uint64_t checksum) 
{
    PacketHeader newheader = {
        .version = MY_PROTOCOOL_VERSION,
        .msg_type = msg_type,
        .data_len = htonl(len),
        .filename_len = htonl(filenamelen),
        .file_size = htonl(filesize)
        .sSum = htonl(checksum)
    };
    //封装数据包转发出去
    size_t totalsize = sizeof(newheader) + len;
    if (totalsize > BUFFER_SIZE) {
        fprintf(stderr, "广播数据包过大(&zu > %d)\n", totalsize, BUFFER_SIZE);
        return;
    }
    memcpy(sendbuffer, &newheader, sizeof(newheader));
    memcpy(sendbuffer + sizeof(newheader), data, len);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket == -1) {//连接成功
            continue;//无效客户端    
        }
        else {
            if (send(clients[i].socket, sendbuffer, totalsize, 0) <= 0) {
                fprintf(stderr, "发送失败到客户端 %d (fd=%d): %s\n", i, clients[i].socket, strerror(errno));
            }
            else {
                //记录成功广播的消息信息
                printf("已广播消息到客户端 %d (fd=%d): type=%u, data_len=%zu", i, clients[i].socket, msg_type, len);
                printf("\n");
            }
        }
       
    }

}

//文件处理
void handle_file_message(Client clients[], int client_fd, PacketHeader header, uint32_t headchecksum) {
    // 给文件名分配空间(暂存文件名）
    char filenamebuffer[MAX_FILENAME_LEN + 1];
    uint32_t filenamelen = header.filename_len;
    if (filenamelen >= MAX_FILENAME_LEN) {
        fprintf(stderr, "文件名称太长\n");
        return;
    }
    //将接收到数据发送到缓冲区
    ssize_t bytes_read = recv(client_fd, filenamebuffer, filenamelen, 0);
    //验证文件名收集长度是否正确
    if (bytes_read != header.filename_len) {
        fprintf(stderr, "文件名称接收不完整（期望：%u，实际：%zd）",
            filenamelen, bytes_read);
        return;
    }
    uint64_t filenamechecksum = calculate_checksum(filenamebuffer, filenamelen);
    filenamebuffer[filenamelen] = '\0'; // 强制终止字符串
    // 检查文件名是否与当前存储的相同
    if (strcmp(client.current_filename, filenamebuffer) == 0) {
        // 文件名相同，跳过文件名处理
        printf("文件名相同，跳过处理\n");
    }
    else {
        // 文件名不同，更新当前文件名
        strncpy(client.current_filename, filenamebuffer, bytes_read);
        client.current_filename[bytes_read] = '\0';
        client.received_size += bytes_read;
        printf("更新文件名为: %s\n", filenamebuffer);
    }
    // 动态分配文件数据内存
    char file_data[BUFFER_SIZE];
    ssize_t total_received = 0;
    uint32_t totalfilesize = header.file_size;

    // 重置校验和（避免多次调用导致残留）
    uint64_t received_checksum = 0;

    while (total_received < totalfilesize) {

        // 计算当前分块的大小
        size_t current_chunk_size = (totalfilesize - total_received > BUFFER_SIZE) ?
            BUFFER_SIZE : (totalfilesize - total_received);

        // 接收当前分块的数据
        ssize_t received = recv(
            client_fd,
            file_data,
            current_chunk_size,
            0
        );
        if (received <= 0) {
            if (received == 0) {
                fprintf(stderr, "连接关闭，文件数据未完全接收\n");
            }
            else {
                perror("recv failed");
            }
            return;
        }
        // **计算接收到的数据的校验和**
        uint64_t chunk_checksum = calculate_checksum(file_data, received);
        uint64_t filechecksum = headchecksum + filenamechecksum + chunk_checksum;
        // 广播当前分块的数据
        broadcast_file_message(clients, client_fd, header,file_data, received,
            MSG_TYPE_FILE, filenamebuffer, filenamelen,
            totalfilesize, filechecksum);
      
        client.received_size += received;
        total_received += received;
    }
}

//文件广播发送
void broadcast_file_message(Client clients[], int sender_fd, PacketHeader header, const void* data, size_t len,
    uint8_t msg_type, const char* filename, uint32_t filenamelen, uint64_t filesize, uint64_t checksum) {

    PacketHeader newheader = {
        .version = MY_PROTOCOOL_VERSION,
        .msg_type = msg_type,
        .data_len = htonl(len),  
        .filename_len = htonl(filenamelen),
        .file_size = htonl(filesize),
        .sSum = htonll(checksum)  // 校验和
        };
    //封装数据包转发出去
    size_t totalsize = sizeof(newheader) + filenamelen + len;
    if (totalsize > BUFFER_SIZE) {
        fprintf(stderr, "广播数据包过大(&zu > %d)\n", totalsize, BUFFER_SIZE);
        return;
    }
    memcpy(sendbuffer, &newheader, sizeof(newheader));
    memcpy(sendbuffer + sizeof(newheader), filename, filenamelen);
    memcpy(sendbuffer + filenamelen + sizeof(newheader), data, len);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket == -1) {//连接成功
            continue;//无效客户端    
        }
        else {
            if (send(clients[i].socket, sendbuffer, totalsize, 0) <= 0) {
                fprintf(stderr, "发送失败到客户端 %d (fd=%d): %s\n", i, clients[i].socket, strerror(errno));
            }
            else {
                //记录成功广播的消息信息
                printf("已广播消息到客户端 %d (fd=%d): type=%u, data_len=%zu", i, clients[i].socket, msg_type, len);
                printf("\n");
            }
        }

    }
}

//主函数
int main() {
    //创建mysql
    MYSQL* conn = mysql_init(NULL);

    if (!conn || !mysql_real_connect(conn, "127.0.0.1", "root", "1228", "server", 0, NULL, 0)) {
        fprintf(stderr, "MySQL init/connect failed: %s\n", mysql_error(conn));
        exit(EXIT_FAILURE);
    }
    //创建用于记录客户端 IP 和端口的表
    if (mysql_query(conn, "DROP TABLE IF EXISTS clients") != 0) {
        fprintf(stderr, "删除旧表失败: %s\n", mysql_error(conn));
    }

    // 创建新表（确保id字段自增）
    if (mysql_query(conn,
        "CREATE TABLE clients ("
        "id INT AUTO_INCREMENT PRIMARY KEY, "
        "ip VARCHAR(15) NOT NULL, "
        "port INT NOT NULL)"
    ) != 0) {
        fprintf(stderr, "创建表失败: %s\n", mysql_error(conn));
        mysql_close(conn);
        exit(EXIT_FAILURE);
    }

    // 解析命令行参数获取端口号
    int port = 8888; // 默认端口
    char port_str[BUFFER_SIZE] = { 0 };
    printf("请输入服务器端口号（默认8888，直接回车使用默认值）: ");
    fflush(stdout);
    if (fgets(port_str, sizeof(port_str), stdin)) {
        char* newline = strchr(port_str, '\n');
        if (newline) *newline = '\0';  // 去掉换行符

        if (port_str[0] != '\0') {
            int input_port = atoi(port_str);
            if (input_port >= 0 && input_port <= 65535) {
                port = input_port;
            }
            else {
                fprintf(stderr, "错误：端口号必须在1024-65535之间，使用默认端口8888\n");
            }
        }
    }
    printf("服务器启动，监听端口 %d\n", port);
    printf("输入 'exit' 退出服务器\n");

    // 初始化客户端数组
    Client clients[MAX_CLIENTS] = { 0 };
    for (int i = 0; i < MAX_CLIENTS; i++) {
        clients[i].socket = -1;//未连接
        clients[i].id = 0;
    }

    // 创建监听Socket TCP
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) error_exit("socket创建失败");

    // 设置端口复用
    // 防止服务无法重启或并发性能受限的问题
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)//SO_REUSEADDR允许重用本地地址（IP + 端口）
        error_exit("setsockopt失败");
    struct sockaddr_in server_addr;//存储服务端的地址信息（IP + 端口）
    memset(&server_addr, 0, sizeof(server_addr));//初始化为0
    server_addr.sin_family = AF_INET;//ipv4
    server_addr.sin_addr.s_addr = INADDR_ANY;//接受所有网卡上的ip地址
    server_addr.sin_port = htons(port);//网络字节序（大端）port
    if (bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0)
        error_exit("bind失败");
    if (listen(listen_fd, MAX_CLIENTS) < 0)
        error_exit("listen失败");
    set_nonblocking(listen_fd);//非阻塞

    // 初始化epoll
    int epoll_fd = epoll_create1(0);//失败返回-1
    if (epoll_fd < 0) error_exit("epoll_create失败");
    //event：配置某个 fd 的监听事件
    //events：保存 epoll_wait 返回的多个就绪事件
    struct epoll_event event, events[MAX_EVENTS];
    //监听服务器的 监听套接字 listen_fd
    event.events = EPOLLIN | EPOLLET;//可读、边缘触发
    event.data.fd = listen_fd;//监听socket
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) < 0)
        error_exit("epoll_ctl添加监听socket失败");

    // 添加标准输入到epoll监控（用于处理exit命令）
    event.data.fd = STDIN_FILENO;//fd字段设置为标准输入的文件描述符
    event.events = EPOLLIN | EPOLLET;//可读、边缘触发
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &event) < 0)//监听event
        error_exit("epoll_ctl添加标准输入失败");

    char exit_cmd[BUFFER_SIZE] = { 0 };
    int should_exit = 0;

    while (!should_exit) {
        int n_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1); 
        //用于等待 I/O 事件的发生,返回就绪事件的数量
        if (n_events < 0) {
            if (errno == EINTR) continue;
            error_exit("epoll_wait失败");
        }

        for (int i = 0; i < n_events; i++) {
            int fd = events[i].data.fd;

            // 处理终端输入（退出命令）
            if (fd == STDIN_FILENO) {
                ssize_t read_size = read(STDIN_FILENO, exit_cmd, sizeof(exit_cmd));
                if (read_size > 0 && strstr(exit_cmd, "exit\n") != NULL) {
                    printf("接收到退出命令，关闭服务器\n");
                    should_exit = 1; // 设置退出标志
                    break; // 跳出事件循环，准备退出
                }
                memset(exit_cmd, 0, sizeof(exit_cmd)); // 清空缓冲区
            }

            // 处理新客户端连接
            else if (fd == listen_fd) {
                struct sockaddr_in client_addr;//存储客户端
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(listen_fd,
                    (struct sockaddr*)&client_addr, &client_len);//接受并返回客户端socket描述符
                if (client_fd < 0) {
                    perror("accept失败");
                    continue;
                }

                // 查找可用客户端槽位
                int client_idx = -1;
                for (int j = 0; j < MAX_CLIENTS; j++) {
                    if (clients[j].socket == -1) {
                        client_idx = j;
                        break;
                    }
                }

                if (client_idx == -1) {
                    close(client_fd);
                    fprintf(stderr, "客户端连接数达到上限（%d）\n", MAX_CLIENTS);
                    continue;
                }

                set_nonblocking(client_fd);//非阻塞
                char client_ip[INET_ADDRSTRLEN];//存储 IPv4 地址
                //客户端的 IPv4地址（二进制形式） 转换为 字符串形式，并存入 client_ip 中
                inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
                int client_port = ntohs(client_addr.sin_port);//转成主机字节序port

                // 使用事务保护的方式插入客户端信息并获取ID
                int new_id = 0;
                if (insert_client_with_id(conn, client_ip, client_port, &new_id)) {
                    printf("新客户端连接: ID=%d IP=%s Port=%d\n", new_id, client_ip, client_port);
                    clients[client_idx].id = new_id; // 使用数据库生成的ID
                }
                else {
                    printf("客户端ID生成失败，拒绝连接: IP=%s Port=%d\n", client_ip, client_port);
                    close(client_fd);
                    continue;
                }
                clients[client_idx].socket = client_fd;
                clients[client_idx].received_size = 0;

                // 添加到epoll监控
                event.data.fd = client_fd;
                event.events = EPOLLIN | EPOLLET;//可读、边缘触发
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event) < 0) {
                    close(client_fd);
                    clients[client_idx].socket = -1;
                    error_exit("epoll_ctl添加客户端失败");
                }
            }

            // 处理客户端数据
            else {
                int client_idx = -1;
                for (int j = 0; j < MAX_CLIENTS; j++) {
                    if (clients[j].socket == fd) {
                        client_idx = j;
                        break;
                    }
                }
                if (client_idx == -1) { close(fd); continue; }

                //uint32_t total_size = sizeof(PacketHeader) + filename_len + len;
                //ssize_t recv_totalsize = recv(fd, recvbuffer,)
                
                PacketHeader header;
                ssize_t recv_size = recv(fd, &header, sizeof(header), 0);
               // ssize_t recv_size = recv(fd, buffer, filename_len, 0);
               // ssize_t recv_size = recv(fd, buffer, data_len, 0);
                if (recv_size != sizeof(header)) {
                    fprintf(stderr, "包头接收不完整\n");
                    exit(1);
                }
                uint64_t headerchecksum = calculate_checksum((char*)&header, sizeof(header));
                if (recv_size <= 0) {
                    // 客户端断开连接
                    printf("客户端%d断开连接（fd=%d）\n", client_idx + 1, fd);
                    close(fd);
                    clients[client_idx].socket = -1;
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
                    continue;
                }

                header.datalen = ntohl(header.datalen);
                header.filename_len = ntohl(header.filename_len);
                header.file_size = ntohll(header.file_size);

                switch (header.msg_type) {
                case MSG_TYPE_TEXT:
                    handle_text_message(clients, fd, header, headerchecksum);
                    break;
                case MSG_TYPE_FILE:
                    handle_file_message(clients, fd, header);
                    break;
                default:
                    fprintf(stderr, "未知消息类型: %d\n", header.msg_type);
                    break;
                }
            }
        }
    }

    // 清理资源
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket != -1) {
            close(clients[i].socket);
            clients[i].socket = -1;
        }
    }
    mysql_close(conn);
    close(listen_fd);
    close(epoll_fd);
    printf("服务器已关闭\n");
    return 0;
}