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

//��������
#define MAX_EVENTS 1024 //epollһ����ദ����¼���
#define MAX_CLIENTS 100 //���ͻ���������
#define BUFFER_SIZE 4096 //ͨ�û�������С
#define MY_PROTOCOOL_VERSION 1 //��ǰЭ��汾
#define MAX_FILENAME_LEN 256  //����ļ�������


//��Ϣ����
#define MSG_TYPE_TEXT 1 //�ı���Ϣ
#define MSG_TYPE_FILE 2 //�ļ���Ϣ

//Э���ͷ
typedef struct {
    uint8_t  version;//1�ֽ�
    uint8_t  msg_type;//1�ֽ�
	uint32_t datalen;//4�ֽ� ��ǰ���ݰ����ݳ���
    uint32_t filename_len;//4�ֽ�
    uint64_t filesize;//8�ֽ� �����ļ��Ĵ�С
    uint64_t sSum;//8�ֽ� ÿ�����ݰ��ĺ�У��
} PacketHeader;

//�ͻ���������Ϣ
typedef struct {
    int socket;
    int id;
    char current_filename[MAX_FILENAME_LEN];//��ǰ�ļ���
    int file_fd;
    size_t received_size; // �ѽ��յ��ļ����ݴ�С
} Client;

char sendbuffer[BUFFER_SIZE];
char recvbuffer[BUFFER_SIZE];
//�����ֽ���
uint64_t htonll(uint64_t value) {
    return ((uint64_t)htonl((uint32_t)(value >> 32)) << 32) | htonl((uint32_t)value);
}
//�����ֽ���
uint64_t ntohll(uint64_t value) {
    return ((uint64_t)ntohl((uint32_t)(value >> 32)) << 32) | ntohl((uint32_t)value);
}

//��������
void error_exit(const char* msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

//�����ļ�Ϊ������ģʽ 
void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL); //F_GETFL��ȡ�ļ���ʶ��
    if (flags == -1) error_exit("fcntl(F_GETFL)");
    flags |= O_NONBLOCK; //���÷����� �첽
    if (fcntl(fd, F_SETFL, flags) == -1) error_exit("fcntl(F_SETFL)");//F_SETFL �����ļ���ʶ��
}

// ����ΨһID������ͻ�����Ϣ�������񱣻���
bool insert_client_with_id(MYSQL* conn, const char* ip, int port, int* client_id) {
    char escaped_ip[BUFFER_SIZE] = { 0 };
    // ת�� IP ��ַ
    mysql_real_escape_string(conn, escaped_ip, ip, strlen(ip));

    // ��������
    if (mysql_query(conn, "START TRANSACTION") != 0) {
        fprintf(stderr, "��������ʧ��: %s\n", mysql_error(conn));
        return false;
    }

    // ������ִ�в������
    char query[BUFFER_SIZE] = { 0 };
    snprintf(query, sizeof(query), "INSERT INTO clients (ip, port) VALUES ('%s', %d)", escaped_ip, port);
    if (mysql_query(conn, query) != 0) {
        fprintf(stderr, "����ʧ��: %s\n", mysql_error(conn));
        if (mysql_query(conn, "ROLLBACK") != 0) {
            fprintf(stderr, "�ع�ʧ��: %s\n", mysql_error(conn));
        }
        return false;
    }

    // ��ȡ���ݿ��Զ����ɵ����� ID
    *client_id = mysql_insert_id(conn);

    // �ύ����
    if (mysql_query(conn, "COMMIT") != 0) {
        fprintf(stderr, "�ύ����ʧ��: %s\n", mysql_error(conn));
        return false;
    }

    return true;
}
// ����У���
uint64_t calculate_checksum(const char* data, size_t len) {
    uint64_t sum = 0;

    for (size_t i = 0; i < len; i++) {
        sum += (uint8_t)data[i];  // ��ÿ���ֽ�ת��Ϊ�޷���8λ�������ۼ�
    }

    return sum;
}
//�ı�����
void handle_text_message(Client clients[], int client_fd, PacketHeader header) {
    //uint32_t len = header.filename_len;
    //����������
    char buffer[BUFFER_SIZE];
    if (header.datalen >= BUFFER_SIZE) {
        fprintf(stderr, "�ı���Ϣ����\n");
        return;
    }
    //�����ı����ݵ���������
    ssize_t bytes_read = recv(client_fd, buffer, header.datalen, MSG_WAITALL);
    //MSG_WAITALL ����ϵͳ�����ܵȴ���ֱ����ȡ��ָ���������ֽ�
    //������ܲ���������
    if (bytes_read != header.datalen) {
        fprintf(stderr, "�ı���Ϣ���ղ�����\n");
        return;
    }
    buffer[bytes_read] = '\0';//��ӽ�����
    //�յ��ı���Ϣ��ӡ����
    printf("�յ��ı���Ϣ: %s\n", buffer);
    broadcast_message(clients, client_fd, buffer, bytes_read, MSG_TYPE_TEXT, NULL, 0, 0);
}

//�ļ�����
void handle_file_message(Client clients[], int client_fd, PacketHeader header) {
    // ���ļ�����������ļ�����
    if (header.filename_len >= MAX_FILENAME_LEN) {
        fprintf(stderr, "�ļ�������\n");
        return;
    }

    // ���ļ�������ռ�
    char filename[MAX_FILENAME_LEN];

    // �����ļ���
    ssize_t bytes_read = recv(client_fd, filename, header.filename_len, MSG_WAITALL);
    if (bytes_read != header.filename_len) {
        fprintf(stderr, "�ļ������ղ�����\n");
        return;
    }

    // ����ļ����Ƿ��뵱ǰ�洢����ͬ
    if (strcmp(client.current_filename, filename) == 0) {
        // �ļ�����ͬ�������ļ�������
        printf("�ļ�����ͬ����������\n");
    }
    else {
        // �ļ�����ͬ�����µ�ǰ�ļ���
        strncpy(client.current_filename, filename, bytes_read);
        client.current_filename[bytes_read] = '\0';
        client.received_size += bytes_read;
        filename[bytes_read] = '\0';
        printf("�����ļ���Ϊ: %s\n", filename);
    }

    // ��̬�����ļ������ڴ�
    char file_data[BUFFER_SIZE];
    ssize_t total_received = 0;
    uint32_t file_data_size = header.filesize;

    while (total_received < file_data_size) {
        // ���㵱ǰ�ֿ�Ĵ�С
        size_t current_chunk_size = (file_data_size - total_received > BUFFER_SIZE) ?
            BUFFER_SIZE : (file_data_size - total_received);

        // ���յ�ǰ�ֿ������
        ssize_t received = recv(
            client_fd,
            file_data,
            current_chunk_size,
            0
        );

        if (received <= 0) {
            if (received == 0) {
                fprintf(stderr, "���ӹرգ��ļ�����δ��ȫ����\n");
            }
            else {
                perror("recv failed");
            }
            return;
        }
        // **������յ������ݵ�У���**
        uint64_t received_checksum = calculate_checksum(file_data, received);

        // **��֤У����Ƿ�ƥ��**
        if (received_checksum != header.sSum) {
            fprintf(stderr, "У��ʹ��󣡽��յ������ݿ������𻵣�\n");
            fprintf(stderr, "����У���: %" PRIu64 ", ʵ��У���: %" PRIu64 "\n",
                header.sSum, received_checksum);
            return;  // У��ʧ�ܣ���ֹ����
        }

        // �㲥��ǰ�ֿ������
        broadcast_message(clients, client_fd, file_data, received,
            MSG_TYPE_FILE, filename, header.filename_len,
            header.filesize, total_received, header.filesize);

        client.received_size += received;
        total_received += received;
    }

    // ����Ƿ���ȫ����
    if (total_received != file_data_size) {
        fprintf(stderr, "�ļ����ݽ��ղ�����\n");
        return;
    }

    // �յ��ļ���Ϣ��ӡ����
    printf("�յ��ļ�: %s (��С: %lu bytes)\n", filename, header.file_size);
}

void broadcast_message(Client clients[], int sender_fd, const void* data, size_t len,
    uint8_t msg_type, const char* filename, uint32_t filename_len, uint64_t file_size,
    uint64_t offset, uint64_t total_size) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket != -1) { // ���ӳɹ�
            PacketHeader header = {
                .version = MY_PROTOCOOL_VERSION,
                .msg_type = msg_type,
                .datalen = htonl(len),
                .filename_len = htonl(filename_len),
                .filesize = htonl(file_size),
                .sSum = htonll(checksum) // ����У���
            };

            // ������Ϣͷ�����ͻ���
            if (send(clients[i].socket, &header, sizeof(header), MSG_NOSIGNAL) == -1) {
                // ʹ��MSG_NOSIGNAL��־�����ڿͻ��˶Ͽ�ʱ����SIGPIPE�ź�
                fprintf(stderr, "Failed to send header to client %d (fd=%d): %s\n",
                    i, clients[i].socket, strerror(errno));
                continue;
            }

            // �����ļ������ͻ���
            if (filename_len > 0) {
                if (send(clients[i].socket, filename, filename_len, MSG_NOSIGNAL) == -1) {
                    fprintf(stderr, "Failed to send filename to client %d (fd=%d): %s\n",
                        i, clients[i].socket, strerror(errno));
                    continue;
                }
            }

            // ����ʵ�����ݵ��ͻ���
            if (send(clients[i].socket, data, len, MSG_NOSIGNAL) == -1) {
                fprintf(stderr, "Failed to send data to client %d (fd=%d): %s\n",
                    i, clients[i].socket, strerror(errno));
                continue;
            }

            // ��¼�ɹ��㲥����Ϣ��Ϣ
            printf("Broadcasted message to client %d (fd=%d): type=%u, data_len=%zu",
                i, clients[i].socket, msg_type, len);

            // ׷���ļ������ļ���С��Ϣ����־
            if (filename_len > 0) {
                printf(", filename=\"%.*s\", file_size=%" PRIu64,
                    filename_len, filename, file_size);
            }

            // ׷�ӷֿ���Ϣ����־
            printf(", offset=%" PRIu64 ", total_size=%" PRIu64 ", checksum=%" PRIu64 "\n",
                offset, total_size, checksum);
        }
    }
}



int main() {
    //����mysql
    MYSQL* conn = mysql_init(NULL);

    if (!conn || !mysql_real_connect(conn, "127.0.0.1", "root", "1228", "server", 0, NULL, 0)) {
        fprintf(stderr, "MySQL init/connect failed: %s\n", mysql_error(conn));
        exit(EXIT_FAILURE);
    }
    //�������ڼ�¼�ͻ��� IP �Ͷ˿ڵı�
    if (mysql_query(conn, "DROP TABLE IF EXISTS clients") != 0) {
        fprintf(stderr, "ɾ���ɱ�ʧ��: %s\n", mysql_error(conn));
    }

    // �����±�ȷ��id�ֶ�������
    if (mysql_query(conn,
        "CREATE TABLE clients ("
        "id INT AUTO_INCREMENT PRIMARY KEY, "
        "ip VARCHAR(15) NOT NULL, "
        "port INT NOT NULL)"
    ) != 0) {
        fprintf(stderr, "������ʧ��: %s\n", mysql_error(conn));
        mysql_close(conn);
        exit(EXIT_FAILURE);
    }

    // ���������в�����ȡ�˿ں�
    int port = 8888; // Ĭ�϶˿�
    char port_str[BUFFER_SIZE] = { 0 };
    printf("������������˿ںţ�Ĭ��8888��ֱ�ӻس�ʹ��Ĭ��ֵ��: ");
    fflush(stdout);
    if (fgets(port_str, sizeof(port_str), stdin)) {
        char* newline = strchr(port_str, '\n');
        if (newline) *newline = '\0';  // ȥ�����з�

        if (port_str[0] != '\0') {
            int input_port = atoi(port_str);
            if (input_port >= 0 && input_port <= 65535) {
                port = input_port;
            }
            else {
                fprintf(stderr, "���󣺶˿ںű�����1024-65535֮�䣬ʹ��Ĭ�϶˿�8888\n");
            }
        }
    }
    printf("�����������������˿� %d\n", port);
    printf("���� 'exit' �˳�������\n");

    // ��ʼ���ͻ�������
    Client clients[MAX_CLIENTS] = { 0 };
    for (int i = 0; i < MAX_CLIENTS; i++) {
        clients[i].socket = -1;//δ����
        clients[i].id = 0;
    }

    // ��������Socket TCP
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) error_exit("socket����ʧ��");

    // ���ö˿ڸ���
    // ��ֹ�����޷������򲢷��������޵�����
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)//SO_REUSEADDR�������ñ��ص�ַ��IP + �˿ڣ�
        error_exit("setsockoptʧ��");
    struct sockaddr_in server_addr;//�洢����˵ĵ�ַ��Ϣ��IP + �˿ڣ�
    memset(&server_addr, 0, sizeof(server_addr));//��ʼ��Ϊ0
    server_addr.sin_family = AF_INET;//ipv4
    server_addr.sin_addr.s_addr = INADDR_ANY;//�������������ϵ�ip��ַ
    server_addr.sin_port = htons(port);//�����ֽ��򣨴�ˣ�port
    if (bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0)
        error_exit("bindʧ��");
    if (listen(listen_fd, MAX_CLIENTS) < 0)
        error_exit("listenʧ��");
    set_nonblocking(listen_fd);//������

    // ��ʼ��epoll
    int epoll_fd = epoll_create1(0);//ʧ�ܷ���-1
    if (epoll_fd < 0) error_exit("epoll_createʧ��");
    //event������ĳ�� fd �ļ����¼�
    //events������ epoll_wait ���صĶ�������¼�
    struct epoll_event event, events[MAX_EVENTS];
    //������������ �����׽��� listen_fd
    event.events = EPOLLIN | EPOLLET;//�ɶ�����Ե����
    event.data.fd = listen_fd;//����socket
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) < 0)
        error_exit("epoll_ctl��Ӽ���socketʧ��");

    // ��ӱ�׼���뵽epoll��أ����ڴ���exit���
    event.data.fd = STDIN_FILENO;//fd�ֶ�����Ϊ��׼������ļ�������
    event.events = EPOLLIN | EPOLLET;//�ɶ�����Ե����
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &event) < 0)//����event
        error_exit("epoll_ctl��ӱ�׼����ʧ��");

    char exit_cmd[BUFFER_SIZE] = { 0 };
    int should_exit = 0;

    while (!should_exit) {
        int n_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        //���ڵȴ� I/O �¼��ķ���,���ؾ����¼�������
        if (n_events < 0) {
            if (errno == EINTR) continue;
            error_exit("epoll_waitʧ��");
        }

        for (int i = 0; i < n_events; i++) {
            int fd = events[i].data.fd;

            // �����ն����루�˳����
            if (fd == STDIN_FILENO) {
                ssize_t read_size = read(STDIN_FILENO, exit_cmd, sizeof(exit_cmd));
                if (read_size > 0 && strstr(exit_cmd, "exit\n") != NULL) {
                    printf("���յ��˳�����رշ�����\n");
                    should_exit = 1; // �����˳���־
                    break; // �����¼�ѭ����׼���˳�
                }
                memset(exit_cmd, 0, sizeof(exit_cmd)); // ��ջ�����
            }

            // �����¿ͻ�������
            else if (fd == listen_fd) {
                struct sockaddr_in client_addr;//�洢�ͻ���
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(listen_fd,
                    (struct sockaddr*)&client_addr, &client_len);//���ܲ����ؿͻ���socket������
                if (client_fd < 0) {
                    perror("acceptʧ��");
                    continue;
                }

                // ���ҿ��ÿͻ��˲�λ
                int client_idx = -1;
                for (int j = 0; j < MAX_CLIENTS; j++) {
                    if (clients[j].socket == -1) {
                        client_idx = j;
                        break;
                    }
                }

                if (client_idx == -1) {
                    close(client_fd);
                    fprintf(stderr, "�ͻ����������ﵽ���ޣ�%d��\n", MAX_CLIENTS);
                    continue;
                }

                set_nonblocking(client_fd);//������
                char client_ip[INET_ADDRSTRLEN];//�洢 IPv4 ��ַ
                //�ͻ��˵� IPv4��ַ����������ʽ�� ת��Ϊ �ַ�����ʽ�������� client_ip ��
                inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
                int client_port = ntohs(client_addr.sin_port);//ת�������ֽ���port

                // ʹ�����񱣻��ķ�ʽ����ͻ�����Ϣ����ȡID
                int new_id = 0;
                if (insert_client_with_id(conn, client_ip, client_port, &new_id)) {
                    printf("�¿ͻ�������: ID=%d IP=%s Port=%d\n", new_id, client_ip, client_port);
                    clients[client_idx].id = new_id; // ʹ�����ݿ����ɵ�ID
                }
                else {
                    printf("�ͻ���ID����ʧ�ܣ��ܾ�����: IP=%s Port=%d\n", client_ip, client_port);
                    close(client_fd);
                    continue;
                }
                clients[client_idx].socket = client_fd;
                clients[client_idx].received_size = 0;

                // ��ӵ�epoll���
                event.data.fd = client_fd;
                event.events = EPOLLIN | EPOLLET;//�ɶ�����Ե����
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event) < 0) {
                    close(client_fd);
                    clients[client_idx].socket = -1;
                    error_exit("epoll_ctl��ӿͻ���ʧ��");
                }
            }

            // ����ͻ�������
            else {
                int client_idx = -1;
                for (int j = 0; j < MAX_CLIENTS; j++) {
                    if (clients[j].socket == fd) {
                        client_idx = j;
                        break;
                    }
                }
                if (client_idx == -1) { close(fd); continue; }

                PacketHeader header = { 0 };
                ssize_t recv_size = recv(fd, &header, sizeof(header), 0);
                if (recv_size <= 0) {
                    // �ͻ��˶Ͽ�����
                    printf("�ͻ���%d�Ͽ����ӣ�fd=%d��\n", client_idx + 1, fd);
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
                    handle_text_message(clients, fd, header);
                    break;
                case MSG_TYPE_FILE:
                    handle_file_message(clients, fd, header);
                    break;
                default:
                    fprintf(stderr, "δ֪��Ϣ����: %d\n", header.msg_type);
                    break;
                }
            }
        }
    }

    // ������Դ
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket != -1) {
            close(clients[i].socket);
            clients[i].socket = -1;
        }
    }
    mysql_close(conn);
    close(listen_fd);
    close(epoll_fd);
    printf("�������ѹر�\n");
    return 0;
}