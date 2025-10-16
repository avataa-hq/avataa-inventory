from sqlalchemy.orm import Session


def from_file(session: Session, file: str = "security/inventory_for_tests.sql"):
    with open(file) as f:
        session.execute(f.read())
    session.commit()
