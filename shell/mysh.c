// Copyright 2022 Zhaoyang Li
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>


typedef struct __node_t {
    char *alias;
    char *command;
    struct __node_t *next;
} node_t;

typedef struct __list_t {
    node_t *head;
} list_t;

void List_Init(list_t *L) {
    L->head = NULL;
}
void List_Lookup(list_t *L, char *alias) {
    node_t *temp = L->head;
    while (temp) {
        if ((strcmp(temp->alias, alias)) == 0) {
            write(1, temp->alias, strlen(temp->alias));
            write(1, " ", strlen(" "));
            write(1, temp->command, strlen(temp->command));
            write(1, "\n", strlen("\n"));
            return;
        }
        temp = temp->next;
    }
}
void List_remove(list_t *L, char *alias) {
    node_t *temp = L->head;
    if ((strcmp(temp->alias, alias)) == 0) {
        L->head = temp->next;
        free(temp->alias);
        free(temp->command);
        free(temp);
    } else {
        node_t *curr = L->head;
        while (curr->next != NULL) {
            // if yes, we need to delete the curr->next node
            if ((strcmp(curr->next->alias, alias) == 0)) {
                temp = curr->next;
                // node will be disconnected from the linked list.
                curr->next = curr->next->next;
                free(temp->alias);
                free(temp->command);
                free(temp);
                break;
            } else {
                curr = curr->next;
            }
        }
    }
}

void List_print(list_t *L) {
    node_t *temp = L->head;
    // int commandLen = 0;
    while (temp) {
        write(1, temp->alias, strlen(temp->alias));
        write(1, " ", strlen(" "));
        write(1, temp->command, strlen(temp->command));
        write(1, "\n", strlen("\n"));
        temp = temp->next;
    }
}

void List_Insert(list_t *L, char *alias, char *command) {
    node_t *new = malloc(sizeof(node_t));
    assert(new);
    new->alias = malloc(strlen(alias) + 1);
    strncpy(new->alias, alias, strlen(alias)+1);
    new->command = malloc(strlen(command) + 1);
    strncpy(new->command, command, strlen(command)+1);
    new->next = L->head;
    L->head = new;
}
char *List_Get(list_t *L, char *alias) {
    alias = strtok(alias, "\n");
    node_t *temp = L->head;
    while (temp) {
        if ((strcmp(temp->alias, alias)) == 0) {
            return temp->command;
        }
        temp = temp->next;
    }
    return NULL;
}

void freeList(list_t *L) {
    node_t *ptr = L->head;
    node_t *temp = 0;
    // int commandLen = 0;
    while (ptr) {
        temp = ptr;
        ptr = ptr->next;
        free(temp->alias);
        free(temp->command);
        free(temp);
    }
    free(L);
}

int count_redirection_sign(char *line) {
    int count = 0;
    char *p_line = line;
    for (int i = 0; i < strlen(line); i++) {
        if (*p_line == '>') {
            count++;
        }
        p_line++;
    }
    return count;
}

char **split_line(char *line, const char *delim, int size) {
    char **tokens;
    tokens =  malloc(sizeof(char *) * size);
    char *token;
    int position = 0;
    token = strtok(line, delim);
    while (token != NULL) {
        tokens[position] = token;
        position++;
        token = strtok(NULL, delim);
        if (position >= size) {
            size += 1;
            tokens = realloc(tokens, size * sizeof(char *));
        }
    }
    tokens[position] = NULL;
    return tokens;
}

int mutiple_file(char *file, const char *delim) {
    char *file_cpoy = strdup(file);
    char *token;
    int count = 0;
    token = strtok(file_cpoy, delim);
    while (token != NULL) {
        count++;
        token = strtok(NULL, delim);
    }
    free(file_cpoy);
    if (count > 1) {
        return 1;
    }
    return 0;
}

void trim(char *str) {
    int i, j, len;
    len = strlen(str);
    for (i = 0; i < len; i++) {
        if (str[0] == ' ' || str[0] == '\t') {
            for (i = 0; i < (len - 1); i++)
                str[i] = str[i + 1];
            str[i] = '\0';
            len--;
            i = -1;
            continue;
        }
        if ((str[i] == ' ' || str[i] == '\t')
         && (str[i + 1] == ' ' || str[i] == '\t')) {
            for (j = i; j < (len - 1); j++) {
                str[j] = str[j + 1];
            }
            str[j] = '\0';
            len--;
            i--;
        }
    }
}

char **parsing(char *line, int *redirection,
 int *redirection_error, char **file_name, int *con) {
    char **tokens;
    char **commands;
    const char s[2] = " ";
    const char redirec_sign[2] = ">";
    trim(line);
    line = strtok(line, "\n");
    if (line == NULL) {
        *con = 1;
        return NULL;
    }
    int redirec_number = 0;
    redirec_number = count_redirection_sign(line);
    if (redirec_number == 0) {
        *redirection = 0;
        *redirection_error = 0;
        commands = split_line(line, s, 1);
        return commands;
    } else if (redirec_number > 1) {
        *redirection = 1;
        *redirection_error = 1;
        return NULL;
    } else {
        if (strncasecmp(line, ">", 1) == 0) {
            *redirection = 1;
            *redirection_error = 1;
            return NULL;
        }
        tokens = split_line(line, redirec_sign, 1);
        if (tokens[1] == NULL || mutiple_file(tokens[1], s)) {
            *redirection = 1;
            *redirection_error = 1;
            free(tokens);
            return NULL;
        }
        *redirection = 1;
        *redirection_error = 0;
        commands = split_line(tokens[0], s, 1);
        tokens[1] = strtok(tokens[1], s);
        *file_name = strdup(tokens[1]);
        free(tokens);
        return commands;
    }
}

void free_command_file(char **commands, char *file_name) {
    if (commands != NULL) {
        free(commands);
    }
    if (file_name != NULL) {
        free(file_name);
    }
}

static char *err = "Usage: mysh [batch-file]\n";

int main(int argc, char *argv[]) {
    FILE *fp = NULL;
    // the input pointer, represent stdin or batch file
    FILE *input_pointer = NULL;
    // Modes: Interactive or Batch
    int mode;         // 0 for Interactice, 1 for Batch
    // Interactive mode
    if (argc == 1) {
        mode = 0;
        input_pointer = stdin;
        write(1, "mysh> ", strlen("mysh> "));
    // Batch mode
    } else if (argc == 2) {
        fp = fopen(argv[1], "r");
        // if can't open file
        if (fp == NULL) {
            // concat error message and filename
            char error_message[200];
            snprintf(error_message, sizeof(error_message),
             "Error: Cannot open file %s.\n", argv[1]);
            write(STDERR_FILENO, error_message, strlen(error_message));
            exit(1);
        }
        input_pointer = fp;
        mode = 1;
     // invalid argument numberss
    } else {
        write(STDERR_FILENO, err, strlen(err));
        exit(1);
    }

    char line[512];  // buffer
    list_t *L = malloc(sizeof(list_t));
    List_Init(L);
    // main program loop
    // get input line
    while (fgets(line, 512, input_pointer) != NULL) {
        // output batch command
        if (mode == 1) {
            write(1, line, strlen(line));
        }
        // parse input line
        int redirection = 0;        // change in parsing
        int redirection_error = 0;  // redirection error
        char *file_name = NULL;
        char **commands = NULL;
        char new_commands[512];
        int con = 0;
        // check list contain this command or not
        if (List_Get(L, line) != NULL) {
            char *new_line;
            new_line = List_Get(L, line);
            strncpy(line, new_line, strlen(new_line)+1);
        }
        commands = parsing(line, &redirection,
         &redirection_error, &file_name, &con);
        if (con == 1) {
            if (mode == 0) {
                write(1, "mysh> ", strlen("mysh> "));
            }
            free_command_file(commands, file_name);
            continue;
        }

        if (!redirection_error && strcmp(commands[0], "exit") == 0) {
            free_command_file(commands, file_name);
            freeList(L);
            if (mode == 1) {
                fclose(input_pointer);
            }
            exit(0);
        }

        // if alias
        if (!redirection_error && ((strcmp(commands[0], "alias") == 0) ||
         ((strcmp(commands[0], "unalias") == 0)))) {
            int commandLen = 0;
            while (commands[commandLen] != NULL) {
                commandLen++;
            }
            // just alias-- Print all
            if ((strcmp(commands[0], "alias") == 0)
             && (commandLen == 1)) {
                List_print(L);
                if (mode == 0) {
                    write(1, "mysh> ", strlen("mysh> "));
                }
                free_command_file(commands, file_name);
                continue;
            }
            // dangerous case
            if ((strcmp(commands[1], "alias") == 0)
             || (strcmp(commands[1], "unalias") == 0)
              || (strcmp(commands[1], "alias") == 0)) {
                write(1, "alias: Too dangerous to alias that.\n",
                 strlen("alias: Too dangerous to alias that.\n"));
                if (mode == 0) {
                    write(1, "mysh> ", strlen("mysh> "));
                }
                free_command_file(commands, file_name);
                continue;
            }
            // alias follow by a word -- Look up
            if (((strcmp(commands[0], "alias")) == 0) && (commandLen == 2)) {
                List_Lookup(L, commands[1]);
                if (mode == 0) {
                    write(1, "mysh> ", strlen("mysh> "));
                }
                free_command_file(commands, file_name);
                continue;
            }
            // unalias follow by a word to remove
            if (((strcmp(commands[0], "unalias")) == 0)) {
                if (commandLen == 2) {
                    List_remove(L, commands[1]);
                    if (mode == 0) {
                        write(1, "mysh> ", strlen("mysh> "));
                    }
                    free_command_file(commands, file_name);
                    continue;
                } else {
                    write(1, "unalias: Incorrect number of arguments.\n",
                     strlen("unalias: Incorrect number of arguments.\n"));
                    if (mode == 0) {
                        write(1, "mysh> ", strlen("mysh> "));
                    }
                    free_command_file(commands, file_name);
                    continue;
                }
            }
            // add new alias to the list
            // If contain, remove then add, else just add
            if (((strcmp(commands[0], "alias")) == 0) && (commandLen >= 3)) {
                strncpy(new_commands, commands[2], strlen(commands[2])+1);
                for (int i = 3; commands[i] != NULL; i++) {
                    strncat(new_commands, " ", 2);
                    strncat(new_commands, commands[i],
                    strlen(commands[i])+1);
                }
                if (List_Get(L, commands[1]) != NULL) {
                    List_remove(L, commands[1]);
                }
                List_Insert(L, commands[1], new_commands);
                if (mode == 0) {
                    write(1, "mysh> ", strlen("mysh> "));
                }
                free_command_file(commands, file_name);
                continue;
            }
        }

        // fork(), execute command
        pid_t pid;
        int status;
        pid = fork();
        if (pid == 0) {
            // file redirection
            if (redirection && !redirection_error) {
                int fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) {
                    char file_error_message[200];
                    snprintf(file_error_message, sizeof(file_error_message),
                     "Cannot write to file %s.\n ", file_name);
                    write(STDERR_FILENO, file_error_message,
                     strlen(file_error_message));
                    free_command_file(commands, file_name);
                    freeList(L);
                    if (mode == 1) {
                        fclose(input_pointer);
                    }
                    _exit(EXIT_FAILURE);
                }
                dup2(fd, fileno(stdout));
                close(fd);
            } else if (redirection && redirection_error) {
                const char error_messge[] = "Redirection misformatted.\n";
                write(STDERR_FILENO, error_messge, strlen(error_messge));
                free_command_file(commands, file_name);
                freeList(L);
                if (mode == 1) {
                    fclose(input_pointer);
                }
                _exit(EXIT_FAILURE);
            }
            // executing command
            execv(commands[0], commands);
            // error and exit if execv failed
            char job_error_message[200];
            snprintf(job_error_message, sizeof(job_error_message),
             "%s: Command not found.\n", commands[0]);
            write(STDERR_FILENO, job_error_message, strlen(job_error_message));
            free_command_file(commands, file_name);
            freeList(L);
            if (mode == 1) {
                fclose(input_pointer);
            }
            _exit(EXIT_FAILURE);
        } else {
            waitpid(pid, &status, WUNTRACED);
        }
        // finished
        if (mode == 0) {
            write(1, "mysh> ", strlen("mysh> "));
        }
        free_command_file(commands, file_name);
    }
    freeList(L);
    fclose(input_pointer);
    return 0;
}
