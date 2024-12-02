from datablunt.tables import DataBluntTable, Primary, Computed, Manual, session

class Video(Manual):
    video_id: Primary[int]
    name: Primary[str]
    path: str | None

class Recording(Manual):
    recording_id: Primary[int]
    recording_date: Primary[str]
    duration: float

class Subject(Manual):
    subject_id: Primary[int]
    name: str

class Session(Computed, parents = [Recording, Subject]):
    session_id: Primary[int]
    session_date: str

    def make(cls, key):
        sub: Subject = session.query(Subject).filter_by(**Subject.valid_keys(key)).one()
        rec: Recording = session.query(Recording).filter_by(**Recording.valid_keys(key)).one()
        print(sub, rec)
        session.add(Session(**key, session_id=100*rec.recording_id, session_date="2023-10-01"))


class Pose(Computed, parents = [Session]):
    frame: int
    
    def make():
        pass


def main():
    Session.populate()

if __name__ == "__main__":
    main()

def add_fake_data():
    for i in range(2, 51):
        # Create new video instances
        new_video = Video(video_id=i, name=f"Sample Video {i}", path=f"/path/to/video_{i}")
        session.add(new_video)

        # Create new recording instances
        new_recording = Recording(recording_id=i, recording_date=f"2023-10-{i:02d}", duration=120.0 + i)
        session.add(new_recording)

        # Create new subject instances
        new_subject = Subject(subject_id=i, name=f"Subject {i}")
        session.add(new_subject)

        # Create new session instances
        if i < 25:
            new_session = Session(session_id=i, session_date=f"2023-10-{i:02d}", recording_id=i, recording_date=f"2023-10-{i:02d}", subject_id=i)
            session.add(new_session)

    # Commit all the new instances
    session.commit()

    # Query to verify entries were added
    added_videos = session.query(Video).all()
    added_recordings = session.query(Recording).all()
    added_subjects = session.query(Subject).all()
    added_sessions = session.query(Session).all()

    print(f"Added {len(added_videos)} videos")
    print(f"Added {len(added_recordings)} recordings")
    print(f"Added {len(added_subjects)} subjects")
    print(f"Added {len(added_sessions)} sessions")