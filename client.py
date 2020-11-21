import rpyc
import sys

DIRECTORY_ADDR = "localhost"
DIRECTORY_PORT = 12345


def create_file(handler, filename, data):
    try:
        handler.create(filename, data)
    except ValueError as e:
        print("File Name Exists; Try another File Name")


def read_file(handler, filename):
    try:
        data = handler.read(filename)
        print(data)
    except ValueError as e:
        print(e)


def write(handler,filename, data):
    handler.write(filename, data)


def delete(handler, filename):
    try:
        handler.delete(filename)
    except ValueError:
        print("Could not delete File. Check if Filename exists.")

def try_handler_connect():
    # Request Connection to Directory
    con_primary = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
    directory = con_primary.root.Directory()
    handler_addr = directory.connect_request_client()

    if handler_addr is None:
        return None
    else:
        print(handler_addr)
        con_handler = rpyc.connect(host=handler_addr[0], port=handler_addr[1])
        handler = con_handler.root.Handler()
        return handler, handler_addr

def main():
    handler_obj = try_handler_connect()

    if handler_obj is None:
        print("No Live Server Found")
    else:
        handler = handler_obj[0]
        handler_addr = handler_obj[1]

        print("Connected to" + str(handler_addr[0]) + ":" + str(handler_addr[1]))
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        take_input = True
        while (take_input):
            arg = input("What would you like to do next? ")

            args = arg.split(" ")
            if args[0] == "exit":
                take_input = False
            # Handle Client operation
            elif args[0] == "create":
                create_file(handler, filename=args[1], data=args[2])
            elif args[0] == "read":
                read_file(handler, filename=args[1])
            elif args[0] == "write":
                write(handler, filename=args[1], data=args[2])
            elif args[0] == "delete":
                delete(handler, filename=args[1])
            else:
                print("Error reading client request")

if __name__ == "__main__":
    #main(sys.argv[1:])
    main()

