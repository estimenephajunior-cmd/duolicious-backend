from typing import (
    Any,
    ClassVar,
    DefaultDict,
    Dict,
    List,
    Optional,
    Annotated,
    Literal,
    Union,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    RootModel,
    field_validator,
    model_validator,
    ValidationError,
    TypeAdapter,
)
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from PIL import Image
from pillow_heif import register_heif_opener
import constants
import io
import base64
from duoaudio import transcode_and_trim_audio_from_base64
import traceback
import antiabuse.antirude.displayname
import antiabuse.antirude.education
import antiabuse.antirude.occupation
import antiabuse.antirude.profile
import antiabuse.bannedphoto
from antiabuse.antispam.urldetector import has_url
from antiabuse.antispam.phonenumberdetector import detect_phone_numbers
from antiabuse.antispam.solicitation import has_solicitation
from util import human_readable_size_metric
from duohash import md5

register_heif_opener()

HEX_COLOR_PATTERN = r"^#[0-9a-fA-F]{6}$"

MIN_NAME_LEN = 1
MAX_NAME_LEN = 64

MIN_ABOUT_LEN = 0
MAX_ABOUT_LEN = 10000

MAX_IMAGE_DIM = 5000
MIN_IMAGE_DIM = 50

MAX_GIF_DIM = 800
MIN_GIF_DIM = 10

MIN_PHOTO_POSITION = 1
MAX_PHOTO_POSITION = 7
REFERRAL_CODE_PATTERN = r"^[A-Z0-9]{6,12}$"
ALLOWED_REGISTRATION_GENDERS = {"Man", "Woman"}


def validate_gif_dimensions(larger_dim: int, smaller_dim: int):
    if larger_dim > MAX_GIF_DIM:
        raise ValueError(
                f'Image must be less than '
                f'{MAX_GIF_DIM}x{MAX_GIF_DIM} '
                'pixels')

    if smaller_dim < MIN_GIF_DIM:
        raise ValueError(
                f'Image must be greater than '
                f'{MIN_GIF_DIM}x{MIN_GIF_DIM} '
                'pixels')


def validate_image_dimensions(larger_dim: int, smaller_dim: int):
    if larger_dim > MAX_IMAGE_DIM:
        raise ValueError(
                f'Image must be less than '
                f'{MAX_IMAGE_DIM}x{MAX_IMAGE_DIM} '
                'pixels')

    if smaller_dim < MIN_IMAGE_DIM:
        raise ValueError(
                f'Image must be greater than '
                f'{MIN_IMAGE_DIM}x{MIN_IMAGE_DIM} '
                'pixels')


class Base64AudioFile(BaseModel):
    base64: str
    transcoded: bytes
    bytes: bytes

    @model_validator(mode='before')
    def convert_base64(cls, values):
        # Avoid performing transcoding a second time
        if 'base64' in values and 'bytes' in values and 'transcoded' in values:
            return values

        response = transcode_and_trim_audio_from_base64(values['base64'])

        if isinstance(response, ValueError):
            raise response

        decoded_bytes, transcoded = response

        values['bytes'] = decoded_bytes
        values['transcoded'] = transcoded

        return values

    class Config:
        arbitrary_types_allowed = True

# Even though this class has a very generic name, it's used exclusively for
# uploading photos
class Base64File(BaseModel):
    position: int = Field(ge=MIN_PHOTO_POSITION, le=MAX_PHOTO_POSITION)
    base64: str
    bytes: bytes
    image: Image.Image
    top: int
    left: int
    md5_hash: str

    @model_validator(mode='before')
    def convert_base64(cls, values):
        try:
            base64_value = values['base64'].split(',')[-1]
        except:
            raise ValueError('Field base64 must be a valid base64 string')

        try:
            decoded_bytes = base64.b64decode(base64_value)
        except base64.binascii.Error as e:
            raise ValueError(f'Field base64 must be a valid base64 string')

        if len(decoded_bytes) > constants.MAX_IMAGE_BYTES:
            raise ValueError(
                f'File must be smaller than '
                f'{human_readable_size_metric(constants.MAX_IMAGE_BYTES)}')

        try:
            image = Image.open(io.BytesIO(decoded_bytes))
        except:
            raise ValueError(f'Base64 string is valid but is not an image')

        try:
            image.load()
        except:
            raise ValueError(f'Image invalid')

        md5_hash = md5(base64_value)
        if antiabuse.bannedphoto.is_banned_photo(md5_hash):
            raise ValueError("That pic breaks the rules 🙈")

        width, height = image.size

        larger_dim = max(width, height)
        smaller_dim = min(width, height)

        if image.format == 'GIF':
            validate_gif_dimensions(larger_dim, smaller_dim)
        else:
            validate_image_dimensions(larger_dim, smaller_dim)

        values['image'] = image
        values['bytes'] = decoded_bytes
        values['md5_hash'] = md5_hash

        return values

    class Config:
        arbitrary_types_allowed = True


class PhotoAssignments(RootModel[Dict[int, int]]):
    @field_validator("root")
    @classmethod
    def validate_root(cls, root: Dict[int, int]) -> Dict[int, int]:
        values = list(root.values())

        if len(values) != len(set(values)):
            raise ValueError('Many photos were assigned to one position')

        for k, v in root.items():
            # Validate that both keys and values are within the allowed range
            if not (MIN_PHOTO_POSITION <= k <= MAX_PHOTO_POSITION):
                raise ValueError('Invalid photo position')
            if not (MIN_PHOTO_POSITION <= v <= MAX_PHOTO_POSITION):
                raise ValueError('Invalid photo position')

            if k == v:
                raise ValueError("Item can't be assigned to itself")

        if not root:
            raise ValueError('Must have at least one assignment')

        return root

    def dict(self, *args, **kwargs) -> Dict[int, int]:  # type: ignore[override]
        """Return the underlying mapping directly, not wrapped in a root key.

        This preserves the behaviour relied upon by callers such as
        `patch_profile_info`, which expect a plain `Dict[int, int]`.
        """
        data = super().dict(*args, **kwargs)
        # Support both Pydantic v1 (`__root__`) and v2 (`root`) styles.
        if 'root' in data:
            return data['root']
        if '__root__' in data:
            return data['__root__']
        # Fallback – should not normally happen, but keeps a sensible type.
        return data  # type: ignore[return-value]


class Theme(BaseModel):
    title_color: str = Field(pattern=HEX_COLOR_PATTERN)
    body_color: str = Field(pattern=HEX_COLOR_PATTERN)
    background_color: str = Field(pattern=HEX_COLOR_PATTERN)


class SessionInfo(BaseModel):
    email: str
    session_token_hash: str
    person_id: Optional[int]
    person_uuid: Optional[str]
    onboarded: bool
    signed_in: bool

    @model_validator(mode='before')
    def set_onboarded(cls, values):
        values['onboarded'] = values.get('person_id') is not None
        return values


class PostAnswer(BaseModel):
    question_id: int
    answer: Optional[bool]
    public: bool


class DeleteAnswer(BaseModel):
    question_id: int


class PostRequestOtp(BaseModel):
    email: EmailStr
    referral_code: Optional[str] = Field(
        default=None,
        pattern=REFERRAL_CODE_PATTERN,
        min_length=6,
        max_length=12,
    )
    jw_quiz_token: Optional[str] = Field(
        default=None,
        min_length=16,
        max_length=256,
    )
    sign_in_only: bool = False

    @field_validator('email', mode='before')
    def validate_email(cls, value):
        return EmailStr._validate(value.lower().strip())

    @field_validator('referral_code', mode='before')
    def validate_referral_code(cls, value):
        if value is None:
            return None
        return str(value).strip().upper()

    @field_validator('jw_quiz_token', mode='before')
    def validate_jw_quiz_token(cls, value):
        if value is None:
            return None
        return str(value).strip()

class PostValidateReferralCode(BaseModel):
    referral_code: str = Field(
        pattern=REFERRAL_CODE_PATTERN,
        min_length=6,
        max_length=12,
    )

    @field_validator('referral_code', mode='before')
    def validate_referral_code(cls, value):
        return str(value).strip().upper()


class PostCheckOtp(BaseModel):
    otp: str = Field(pattern=r"^\d{6}$")

    @field_validator('otp', mode='before')
    def strip_otp(cls, value):
        return str(value).strip()


class PostStartJwQuiz(BaseModel):
    email: EmailStr
    referral_code: str = Field(
        pattern=REFERRAL_CODE_PATTERN,
        min_length=6,
        max_length=12,
    )

    @field_validator('email', mode='before')
    def validate_email(cls, value):
        return EmailStr._validate(value.lower().strip())

    @field_validator('referral_code', mode='before')
    def validate_referral_code(cls, value):
        return str(value).strip().upper()


class JwQuizAnswerItem(BaseModel):
    question_id: int = Field(ge=1)
    selected_option: str = Field(min_length=1, max_length=256)

    @field_validator('selected_option', mode='before')
    def validate_selected_option(cls, value):
        return str(value).strip()


class PostCompleteJwQuiz(BaseModel):
    jw_quiz_token: str = Field(min_length=16, max_length=256)
    answers: list[JwQuizAnswerItem] = Field(default_factory=list, min_length=1, max_length=12)

    @field_validator('jw_quiz_token', mode='before')
    def validate_jw_quiz_token(cls, value):
        return str(value).strip()


class PatchOnboardeeInfo(BaseModel):
    referral_code: Optional[str] = Field(
        default=None,
        pattern=REFERRAL_CODE_PATTERN,
        min_length=6,
        max_length=12,
    )
    name: Optional[str] = Field(
        default=None,
        min_length=MIN_NAME_LEN,
        max_length=MAX_NAME_LEN,
    )
    date_of_birth: Optional[str] = None
    baptism_date: Optional[str] = None
    location: Optional[str] = Field(default=None, min_length=1)
    congregation_id: Optional[int] = Field(default=None, ge=1)
    gender: Optional[str] = Field(default=None, min_length=1)
    other_peoples_genders: Optional[List[str]] = None
    base64_file: Optional[Base64File] = None

    @field_validator('name', mode='before')
    @classmethod
    def strip_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return value.strip()

    @field_validator('other_peoples_genders')
    @classmethod
    def validate_other_peoples_genders(
        cls,
        value: Optional[List[str]],
    ) -> Optional[List[str]]:
        if value is None:
            return value
        for gender in value:
            if len(gender) < 1:
                raise ValueError('each gender must be at least 1 character long')
            if gender not in ALLOWED_REGISTRATION_GENDERS:
                raise ValueError('Only Man or Woman are allowed')
        return value

    @field_validator('referral_code', mode='before')
    @classmethod
    def normalize_referral_code(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return str(value).strip().upper()

    @field_validator('gender')
    @classmethod
    def validate_gender(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        if value not in ALLOWED_REGISTRATION_GENDERS:
            raise ValueError('Only Man or Woman are allowed')
        return value

    @field_validator('date_of_birth', 'baptism_date')
    def validate_date_string(cls, date_of_birth):
        if date_of_birth is None:
            return date_of_birth
        datetime.strptime(date_of_birth, '%Y-%m-%d').date()
        return date_of_birth

    @field_validator('name')
    def name_must_not_be_rude(cls, value):
        if value is None:
            return value
        if antiabuse.antirude.displayname.is_rude(value):
            raise ValueError('Too rude')
        return value

    @model_validator(mode='after')
    def check_exactly_one(self):
        if len(self.__pydantic_fields_set__) != 1:
            raise ValueError('Exactly one value must be set')

        [field_name] = self.__pydantic_fields_set__
        field_value = getattr(self, field_name)

        if field_value is None:
            raise ValueError(f'Field {field_name} must not be None')

        return self

    class Config:
        arbitrary_types_allowed = True


class DeleteOnboardeeInfo(BaseModel):
    files: List[int]

    @field_validator("files")
    @classmethod
    def validate_files(cls, files: List[int]) -> List[int]:
        for pos in files:
            if not (MIN_PHOTO_POSITION <= pos <= MAX_PHOTO_POSITION):
                raise ValueError("Invalid photo position")
        return files


class DeleteProfileInfo(BaseModel):
    files: Optional[List[int]] = Field(
        default=None,
        min_length=1,
        max_length=MAX_PHOTO_POSITION,
    )

    audio_files: Optional[List[int]] = Field(
        default=None,
        min_length=1,
        max_length=1,
    )

    @field_validator("files")
    @classmethod
    def validate_files(
        cls,
        files: Optional[List[int]],
    ) -> Optional[List[int]]:
        if files is None:
            return files
        for pos in files:
            if not (MIN_PHOTO_POSITION <= pos <= MAX_PHOTO_POSITION):
                raise ValueError("Invalid photo position")
        return files

    @field_validator("audio_files")
    @classmethod
    def validate_audio_files(
        cls,
        audio_files: Optional[List[int]],
    ) -> Optional[List[int]]:
        if audio_files is None:
            return audio_files
        for value in audio_files:
            if value != -1:
                raise ValueError("Audio file positions must be -1")
        return audio_files


class PatchProfileInfo(BaseModel):
    base64_file: Optional[Base64File] = None
    base64_audio_file: Optional[Base64AudioFile] = None
    photo_assignments: Optional[PhotoAssignments] = None
    name: Optional[str] = Field(
        default=None,
        min_length=MIN_NAME_LEN,
        max_length=MAX_NAME_LEN,
    )
    about: Optional[str] = Field(
        default=None,
        min_length=MIN_ABOUT_LEN,
        max_length=MAX_ABOUT_LEN,
    )
    gender: Optional[str] = None
    orientation: Optional[str] = None
    ethnicity: Optional[str] = None
    location: Optional[str] = None
    occupation: Optional[str] = Field(default=None, min_length=1, max_length=64)
    education: Optional[str] = Field(default=None, min_length=1, max_length=64)
    height: Optional[int] = None
    looking_for: Optional[str] = None
    smoking: Optional[str] = None
    drinking: Optional[str] = None
    drugs: Optional[str] = None
    long_distance: Optional[str] = None
    relationship_status: Optional[str] = None
    pioneer_status: Optional[str] = None
    service_goals: Optional[str] = Field(default=None, min_length=1, max_length=160)
    willingness_to_relocate: Optional[str] = Field(default=None, min_length=1, max_length=120)
    family_worship_habit: Optional[str] = Field(default=None, min_length=1, max_length=120)
    spiritual_routine: Optional[str] = Field(default=None, min_length=1, max_length=120)
    willing_to_involve_family_early: Optional[str] = Field(default=None, min_length=1, max_length=120)
    open_to_chaperoned_video_calls: Optional[str] = Field(default=None, min_length=1, max_length=120)
    congregation_compatibility: Optional[str] = Field(default=None, min_length=1, max_length=160)
    service_lifestyle: Optional[str] = Field(default=None, min_length=1, max_length=160)
    life_stage: Optional[str] = Field(default=None, min_length=1, max_length=120)
    emotional_temperament: Optional[str] = Field(default=None, min_length=1, max_length=120)
    communication_style: Optional[str] = Field(default=None, min_length=1, max_length=120)
    has_kids: Optional[str] = None
    wants_kids: Optional[str] = None
    exercise: Optional[str] = None
    religion: Optional[str] = None
    star_sign: Optional[str] = None
    units: Optional[str] = None
    chats: Optional[str] = None
    intros: Optional[str] = None
    verification_level: Optional[str] = None
    profile_status: Optional[str] = None
    show_my_location: Optional[str] = None
    show_my_age: Optional[str] = None
    hide_me_from_strangers: Optional[str] = None
    browse_invisibly: Optional[str] = None
    who_can_contact_me: Optional[str] = Field(default=None, min_length=1, max_length=120)
    request_format_preference: Optional[str] = Field(default=None, min_length=1, max_length=120)
    message_pace_preference: Optional[str] = Field(default=None, min_length=1, max_length=120)
    theme: Optional[Theme] = None

    @model_validator(mode='after')
    def check_exactly_one(self):
        if len(self.__pydantic_fields_set__) != 1:
            raise ValueError('Exactly one value must be set')

        [field_name] = self.__pydantic_fields_set__
        field_value = getattr(self, field_name)

        if field_value is None:
            raise ValueError(f'Field {field_name} must not be None')

        return self

    @model_validator(mode='before')
    def strip_strs(cls, values):
        for key, val in values.items():
            values[key] = val.strip() if type(val) is str else val

        return values

    @field_validator('name')
    def name_must_not_be_rude(cls, value):
        if value is None:
            return value
        if antiabuse.antirude.displayname.is_rude(value):
            raise ValueError('Too rude')
        return value

    @field_validator('gender')
    def gender_must_be_allowed(cls, value):
        if value is None:
            return value
        if value not in ALLOWED_REGISTRATION_GENDERS:
            raise ValueError('Only Man or Woman are allowed')
        return value

    @field_validator('about')
    def about_must_not_be_rude(cls, value):
        if value is None:
            return value
        if antiabuse.antirude.profile.is_rude(value):
            raise ValueError('Too rude')
        return value

    @field_validator('about')
    def about_must_not_have_spam(cls, value):
        if value is None:
            return value
        if \
                has_url(value) or \
                detect_phone_numbers(value) or \
                has_solicitation(value):
            raise ValueError('Spam')
        return value

    @field_validator('profile_status')
    def profile_status_must_be_allowed(cls, value):
        if value is None:
            return value
        if value not in {'active', 'paused', 'serious'}:
            raise ValueError('Invalid profile status')
        return value

    @field_validator('occupation')
    def occupation_must_not_be_rude(cls, value):
        if value is None:
            return value
        if antiabuse.antirude.occupation.is_rude(value):
            raise ValueError('Too rude')
        return value

    @field_validator('education')
    def education_must_not_be_rude(cls, value):
        if value is None:
            return value
        if antiabuse.antirude.education.is_rude(value):
            raise ValueError('Too rude')
        return value

    @field_validator(
        'service_goals',
        'willingness_to_relocate',
        'family_worship_habit',
        'spiritual_routine',
        'willing_to_involve_family_early',
        'open_to_chaperoned_video_calls',
        'congregation_compatibility',
        'service_lifestyle',
        'life_stage',
        'emotional_temperament',
        'communication_style',
        'who_can_contact_me',
        'request_format_preference',
        'message_pace_preference',
    )
    def marriage_minded_fields_must_not_be_rude(cls, value):
        if value is None:
            return value
        if antiabuse.antirude.profile.is_rude(value):
            raise ValueError('Too rude')
        return value

    class Config:
        arbitrary_types_allowed = True


class PostSearchFilter(BaseModel):
    class Age(BaseModel):
        min_age: Optional[int]
        max_age: Optional[int]

    class Height(BaseModel):
        min_height_cm: Optional[int]
        max_height_cm: Optional[int]

    gender: Optional[List[str]] = Field(default=None, min_length=1)
    ethnicity: Optional[List[str]] = Field(default=None, min_length=1)
    city: Optional[str] = Field(default=None, min_length=1, max_length=120)
    state: Optional[str] = Field(default=None, min_length=1, max_length=120)
    age: Optional[Age] = None
    baptism_years: Optional[int] = Field(default=None, ge=2, le=80)
    furthest_distance: Optional[int] = None
    height: Optional[Height] = None
    has_a_profile_picture: Optional[List[str]] = Field(default=None, min_length=1)
    looking_for: Optional[List[str]] = Field(default=None, min_length=1)
    drinking: Optional[List[str]] = Field(default=None, min_length=1)
    long_distance: Optional[List[str]] = Field(default=None, min_length=1)
    relationship_status: Optional[List[str]] = Field(default=None, min_length=1)
    pioneer_status: Optional[List[str]] = Field(default=None, min_length=1)
    has_kids: Optional[List[str]] = Field(default=None, min_length=1)
    wants_kids: Optional[List[str]] = Field(default=None, min_length=1)
    exercise: Optional[List[str]] = Field(default=None, min_length=1)
    religion: Optional[List[str]] = Field(default=None, min_length=1)
    star_sign: Optional[List[str]] = Field(default=None, min_length=1)

    @model_validator(mode='before')
    def strip_strings(cls, values):
        for key in ('city', 'state'):
            if key in values and values[key] is not None:
                values[key] = str(values[key]).strip()
        return values

    people_you_messaged: Optional[str] = None
    people_you_skipped: Optional[str] = None

    @model_validator(mode='after')
    def check_exactly_one(self):
        if len(self.__pydantic_fields_set__) != 1:
            raise ValueError('Exactly one value must be set')

        [field_name] = self.__pydantic_fields_set__
        field_value = getattr(self, field_name)

        if field_name == 'furthest_distance':
            pass
        elif field_value is None:
            raise ValueError(f'Field {field_name} must not be None')

        return self

    class Config:
        arbitrary_types_allowed = True


class PostSearchFilterAnswer(BaseModel):
    question_id: int
    answer: Optional[bool]
    accept_unanswered: bool


class PostInboxInfo(BaseModel):
    person_uuids: List[str]


class PostSkip(BaseModel):
    report_category: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
    )
    report_context: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=200,
    )
    report_reason: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=10000,
    )
    base64_file: Optional[Base64File] = None

    @field_validator('report_reason', mode='before')
    @classmethod
    def strip_report_reason(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return value.strip()

    @field_validator('report_category', 'report_context', mode='before')
    @classmethod
    def strip_report_metadata(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return value.strip()


class PostVerificationSelfie(BaseModel):
    base64_file: Base64File


class PostVerificationDocument(BaseModel):
    kind: Literal['government_id', 'witness', 'supporting_photo']
    label: Optional[str] = Field(default=None, min_length=1, max_length=120)
    base64_file: Base64File

    @field_validator('label', mode='before')
    @classmethod
    def strip_label(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return value.strip()


class PostIntroReviewAction(BaseModel):
    prompt: Optional[str] = Field(default=None, min_length=1, max_length=1000)

    @field_validator('prompt', mode='before')
    @classmethod
    def strip_prompt(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return value.strip()


class PatchCourtshipState(BaseModel):
    first_call_status: Optional[Literal['not-planned', 'planning', 'scheduled', 'completed']] = None
    first_call_timing: Optional[str] = Field(default=None, max_length=240)
    first_call_plan: Optional[str] = Field(default=None, max_length=1000)
    ready_for_video_call: Optional[bool] = None
    ready_for_family_introduction: Optional[bool] = None
    dismiss_prompt: Optional[bool] = None

    @field_validator('first_call_timing', 'first_call_plan', mode='before')
    @classmethod
    def strip_courtship_text(cls, value):
        if value is None:
            return value
        return str(value).strip()

    @model_validator(mode='after')
    def check_at_least_one(self):
        if not self.__pydantic_fields_set__:
            raise ValueError('At least one value must be set')
        return self


class IntroGateQuestionItem(BaseModel):
    id: Optional[int] = None
    prompt: str = Field(min_length=1, max_length=240)
    answer_type: Literal['yes_no', 'free_text']
    response_mode: Literal['text', 'voice', 'both'] = 'text'
    is_required: bool = True
    ordinal: int = Field(ge=0, le=9)
    is_active: bool = True

    @field_validator('prompt', mode='before')
    @classmethod
    def strip_prompt_text(cls, value):
        return str(value or '').strip()


class PutIntroGateQuestions(BaseModel):
    questions: list[IntroGateQuestionItem] = Field(default_factory=list, max_length=10)


class IntroRequestAnswerItem(BaseModel):
    question_id: Optional[int] = None
    prompt_snapshot: str = Field(default='', max_length=240)
    answer_type: Literal['yes_no', 'free_text']
    response_mode: Literal['text', 'voice', 'both'] = 'text'
    answer_text: Optional[str] = Field(default='', max_length=1000)
    answer_bool: Optional[bool] = None
    base64_audio_file: Optional[Base64AudioFile] = None

    @field_validator('prompt_snapshot', 'answer_text', mode='before')
    @classmethod
    def strip_answer_text(cls, value):
        return '' if value is None else str(value).strip()


class PostIntroRequest(BaseModel):
    note: Optional[str] = Field(default='', max_length=1000)
    reason_for_reaching_out: Optional[str] = Field(default='', max_length=1000)
    why_we_may_match: Optional[str] = Field(default='', max_length=600)
    base64_audio_file: Optional[Base64AudioFile] = None
    answers: list[IntroRequestAnswerItem] = Field(default_factory=list, max_length=10)

    @field_validator('note', 'reason_for_reaching_out', 'why_we_may_match', mode='before')
    @classmethod
    def strip_note(cls, value):
        return '' if value is None else str(value).strip()


class PostExternalReport(BaseModel):
    reporter_name: str = Field(min_length=1, max_length=120)
    reporter_email: EmailStr
    relationship_to_user: str = Field(min_length=1, max_length=120)
    target_name: str = Field(min_length=1, max_length=120)
    target_email: Optional[str] = Field(default='', max_length=320)
    target_profile_url: Optional[str] = Field(default='', max_length=500)
    claim: str = Field(min_length=10, max_length=5000)
    evidence_details: str = Field(min_length=10, max_length=10000)
    base64_file: Optional[Base64File] = None

    @field_validator(
        'reporter_name',
        'relationship_to_user',
        'target_name',
        'target_email',
        'target_profile_url',
        'claim',
        'evidence_details',
        mode='before',
    )
    @classmethod
    def strip_text_fields(cls, value):
        if value is None:
            return value
        return str(value).strip()

    @field_validator('reporter_email', mode='before')
    def validate_reporter_email(cls, value):
        return EmailStr._validate(value.lower().strip())


class Base64Attachment(BaseModel):
    base64: str
    bytes: bytes
    mime_type: str = Field(min_length=1, max_length=120)
    file_name: str = Field(min_length=1, max_length=255)

    @model_validator(mode='before')
    def convert_base64(cls, values):
        try:
            base64_value = values['base64'].split(',')[-1]
        except Exception:
            raise ValueError('Attachment must be a valid base64 string')

        try:
            decoded_bytes = base64.b64decode(base64_value)
        except base64.binascii.Error:
            raise ValueError('Attachment must be a valid base64 string')

        max_attachment_bytes = 10 * 1024 * 1024
        if len(decoded_bytes) > max_attachment_bytes:
            raise ValueError(
                f'Attachment must be smaller than '
                f'{human_readable_size_metric(max_attachment_bytes)}'
            )

        values['bytes'] = decoded_bytes
        values['mime_type'] = str(values.get('mime_type') or '').strip()
        values['file_name'] = str(values.get('file_name') or '').strip()

        if not values['mime_type']:
            raise ValueError('Attachment mime_type is required')

        if not values['file_name']:
            raise ValueError('Attachment file_name is required')

        return values


class PostAdminSupportMessage(BaseModel):
    body: Optional[str] = Field(default='', max_length=4000)
    attachment: Optional[Base64Attachment] = None

    @field_validator('body', mode='before')
    @classmethod
    def strip_body(cls, value):
        return '' if value is None else str(value).strip()

    @model_validator(mode='after')
    def require_body_or_attachment(self):
        if not self.body and self.attachment is None:
            raise ValueError('Message body or attachment is required')
        return self


class ValidDatetime(BaseModel):
    datetime: datetime

    @field_validator('datetime', mode='before')
    def _validate_iso8601(cls, v):
        """
        Allow None or anything `datetime.fromisoformat` can parse.
        Accept the common trailing ‘Z’ (UTC) designator as well.
        """
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('`datetime` must be an ISO-8601 datetime')

class RevenuecatBase(BaseModel):
    model_config = ConfigDict(extra='ignore')

class InitialPurchaseEvent(RevenuecatBase):
    type: Literal['INITIAL_PURCHASE']
    app_user_id: str

class RenewalEvent(RevenuecatBase):
    type: Literal['RENEWAL']
    app_user_id: str

class ExpirationEvent(RevenuecatBase):
    type: Literal['EXPIRATION']
    app_user_id: str

class TransferEvent(RevenuecatBase):
    type: Literal['TRANSFER']
    transferred_to: List[str]
    transferred_from: List[str]

RevenuecatEvent = Annotated[
    Union[InitialPurchaseEvent, RenewalEvent, ExpirationEvent, TransferEvent],
    Field(discriminator='type'),
]

class PostRevenuecat(BaseModel):
    model_config = ConfigDict(extra='ignore')

    api_version: Optional[str] = None
    event: Optional[RevenuecatEvent] = None
    # keep the raw payload for unknown types (or failed parses)
    raw_event: Optional[Dict[str, Any]] = None
    raw_event_error: Optional[str] = None

    # one adapter reused for all validations
    _EVENT_ADAPTER: ClassVar[TypeAdapter] = TypeAdapter(RevenuecatEvent)

    @model_validator(mode='before')
    @classmethod
    def _coerce_event(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        ev = values.get('event')
        if isinstance(ev, dict):
            try:
                # Try to parse into one of the known discriminated variants
                values['event'] = cls._EVENT_ADAPTER.validate_python(ev)
            except ValidationError as e:
                # Unknown type or bad payload → stash and null out event
                values['raw_event'] = ev
                values['raw_event_error'] = str(e)
                values['event'] = None
        return values


class PostMarkVisitorsChecked(BaseModel):
    time: Optional[datetime] = None
