import rpyc
import sys

DIRECTORY_ADDR = "localhost"
DIRECTORY_PORT = 12345


def create_file(handler, filename, data):
    handler.create(filename, data)


def read_file(handler, filename):
    data = handler.read(filename)
    print(data)


def main(args):
    # Request Connection to Directory
    con_primary = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
    directory = con_primary.root.Directory()
    handler_addr = directory.connect_request_client()

    # Connect to Handler
    con_handler = rpyc.connect(host=handler_addr[0], port=handler_addr[1])
    handler = con_handler.root.Handler()

    print("Connected to" + str(handler_addr[0]) + ":" + str(handler_addr[1]))

    # Handle Client operation
    if args[0] == "create":
        create_file(handler, filename=args[1], data=args[2])
    elif args[0] == "read":
        read_file(handler, filename=args[1])
    else:
        print("Error reading client request")
        exit()


if __name__ == "__main__":
    main(sys.argv[1:])




